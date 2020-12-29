package raftkv

import (
	"labgob"
	"labrpc"
	"log"
	"raft"
	"sync/atomic"
	"time"
	"util"
)

// opEntry is a log entry in raft
type opEntry struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Type      string // Put, Append, Get
	Key       string
	Value     string
	ClientID  int64
	RequestID int64
}

type snapshot struct {
	KVMap     map[string]string // key-value map
	ClientACK map[int64]int64   // client ID -> the highest ACKed request ID
}

type commitCallbackInfo struct {
	callback func(bool)
	term     int
}

// KVServer provides RPC service
type KVServer struct {
	mu      util.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.

	killed         bool
	killCh         chan struct{}                // channel signals kill
	commitCallback map[int][]commitCallbackInfo // commit index -> corresponding callback
	kvMap          map[string]string            // key-value map
	clientACK      map[int64]int64              // client ID -> the highest ACKed request ID
	term           int                          // raft term
	index          int                          // raft index
}

// NOTE: callback is called AFTER op applied, with mutex held
func (kv *KVServer) registerCommitCallback(index, term int, callback func(bool)) {
	kv.mu.AssertHeld()
	kv.commitCallback[index] = append(kv.commitCallback[index],
		commitCallbackInfo{
			term:     term,
			callback: callback,
		})
}

func cloneMapStringString(m map[string]string) map[string]string {
	r := make(map[string]string)
	for k, v := range m {
		r[k] = v
	}
	return r
}

func cloneMapInt64Int64(m map[int64]int64) map[int64]int64 {
	r := make(map[int64]int64)
	for k, v := range m {
		r[k] = v
	}
	return r
}

// generate snapshot
// deep copy current state
func (kv *KVServer) generateSnapshot() snapshot {
	kv.mu.AssertHeld()
	snap := snapshot{
		KVMap:     cloneMapStringString(kv.kvMap),
		ClientACK: cloneMapInt64Int64(kv.clientACK),
	}
	return snap
}

func (kv *KVServer) checkShouldSnapshot() {
	kv.mu.AssertHeld()
	if kv.maxraftstate == -1 {
		return
	}
	sz := kv.rf.GetStateSize()
	if sz >= kv.maxraftstate {
		// generate snapshot, call raft
		snap := kv.generateSnapshot()
		// log.Printf("Server #%d: snap ack=%v", kv.me, snap.ClientACK)
		kv.rf.UpdateSnapshot(kv.term, kv.index, snap)
	}
}

func (kv *KVServer) raftExecute(op opEntry, commitCallback func()) (isleader bool, committed bool) {
	kv.mu.Lock()

	if kv.killed {
		kv.mu.Unlock()
		return false, false
	}

	index, term, isleader := kv.rf.Start(op)
	if !isleader {
		kv.mu.Unlock()
		return false, false
	}

	DPrintf("Server #%d: leader executing op %v", kv.me, op)

	// flag is used to eliminate race condition
	// between callback & timeout. whoever captured
	// the flag is allowed to progress
	flag := int32(0)

	// channel used to notify completion
	ch := make(chan struct{}, 1)

	kv.registerCommitCallback(index, term, func(success bool) {
		kv.mu.AssertHeld()
		if success && atomic.CompareAndSwapInt32(&flag, 0, 1) {
			// commit succeeded and flag captured, execute callback
			if commitCallback != nil {
				commitCallback()
			}
			ch <- struct{}{}
		} else {
			// commit failed or flag not captured
			// close channel to notify failure
			close(ch)
		}
	})

	kv.mu.Unlock()

	select {
	case _, ok := <-ch:
		return true, ok
	case <-time.After(200 * time.Millisecond):
		if atomic.CompareAndSwapInt32(&flag, 0, 1) {
			// flag captured, actual timeout
			return false, false
		}
		// flag not captured, callback succeeded
		_, ok := <-ch
		return true, ok
	}
}

// Get RPC call
func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	// DPrintf("Server #%d: Get %s", kv.me, args.Key)

	var value string
	isleader, committed := kv.raftExecute(
		opEntry{
			Type:      "Read",
			ClientID:  args.ClientID,
			RequestID: args.RequestID,
		},
		func() {
			value = kv.kvMap[args.Key]
		})

	if !isleader {
		*reply = GetReply{WrongLeader: true}
		return
	}

	if !committed {
		*reply = GetReply{Err: "commit failed"}
		return
	}

	*reply = GetReply{Value: value}
}

// PutAppend RPC call
func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.

	if !(args.Op == "Put" || args.Op == "Append") {
		*reply = PutAppendReply{Err: "invalid op name"}
		return
	}

	// DPrintf("Server #%d: %s %s %s", kv.me, args.Op, args.Key, args.Value)

	isleader, committed := kv.raftExecute(opEntry{
		Type:      args.Op,
		Key:       args.Key,
		Value:     args.Value,
		ClientID:  args.ClientID,
		RequestID: args.RequestID,
	}, nil)

	if !isleader {
		*reply = PutAppendReply{WrongLeader: true}
		return
	}

	if !committed {
		*reply = PutAppendReply{Err: "commit failed"}
		return
	}

	*reply = PutAppendReply{}
}

// Kill the server.
// the tester calls Kill() when a KVServer instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *KVServer) Kill() {
	kv.killCh <- struct{}{}

	kv.mu.Lock()
	kv.killed = true
	kv.mu.Unlock()

	kv.rf.Kill()
}

func (kv *KVServer) apply(op opEntry) (wrongRequestID bool) {
	kv.mu.AssertHeld()

	ack := kv.clientACK[op.ClientID]
	if ack >= op.RequestID {
		return false
	}

	if ack+1 != op.RequestID {
		return true
	}

	kv.clientACK[op.ClientID] = op.RequestID

	switch op.Type {
	case "Read":
		break
	case "Put":
		kv.kvMap[op.Key] = op.Value
		break
	case "Append":
		kv.kvMap[op.Key] += op.Value
		break
	default:
		log.Panicf("not supported op %s", op.Type)
	}

	return false
}

func (kv *KVServer) applySnapshot(snap snapshot) {
	kv.mu.AssertHeld()
	// clone might not be needed
	kv.kvMap = cloneMapStringString(snap.KVMap)
	kv.clientACK = cloneMapInt64Int64(snap.ClientACK)
}

func (kv *KVServer) startApplyWorker() {
	go func() {
		for {
			select {
			case <-kv.killCh:
				kv.mu.Lock()
				for _, cbs := range kv.commitCallback {
					for _, cb := range cbs {
						cb.callback(false)
					}
				}
				kv.mu.Unlock()
				return
			case msg := <-kv.applyCh:
				if msg.CommandValid {
					kv.mu.Lock()
					op := msg.Command.(opEntry)
					DPrintf("Server #%d: applying command, term=%d, index=%d, cmdTerm=%d, cmdIndex=%d",
						kv.me, kv.term, kv.index, msg.CommandTerm, msg.CommandIndex)
					kv.term = msg.CommandTerm
					kv.index = msg.CommandIndex
					// apply the message
					wrongRequestID := kv.apply(op)
					// execute commit callback if exist
					if cbs, ok := kv.commitCallback[msg.CommandIndex]; ok {
						delete(kv.commitCallback, msg.CommandIndex)
						for _, cb := range cbs {
							cb.callback(cb.term == msg.CommandTerm && !wrongRequestID)
						}
					}
					kv.checkShouldSnapshot()
					kv.mu.Unlock()
				} else if msg.SnapshotValid {
					kv.mu.Lock()
					DPrintf("Server #%d: applying snapshot, term=%d, index=%d, snapTerm=%d, snapIndex=%d",
						kv.me, kv.term, kv.index, msg.SnapshotLastTerm, msg.SnapshotLastIndex)
					util.Assert(msg.SnapshotLastTerm >= kv.term &&
						msg.SnapshotLastIndex >= kv.index, "term & index should be monotonic")
					prevIndex := kv.index
					kv.term, kv.index = msg.SnapshotLastTerm, msg.SnapshotLastIndex
					// remove previous callback (avoid memory leak)
					for i := prevIndex; i <= kv.index; i++ {
						delete(kv.commitCallback, i)
					}
					// apply snapshot
					snap := msg.Snapshot.(snapshot)
					// log.Printf("Server #%d: snap ack=%v", kv.me, snap.ClientACK)
					kv.applySnapshot(snap)
					kv.mu.Unlock()
				} else {
					// TODO: command not valid?
					panic("not supported yet")
				}
			}
		}
	}()
}

// StartKVServer starts the server.
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(opEntry{})
	labgob.Register(snapshot{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.

	// start the apply worker
	kv.killCh = make(chan struct{})
	kv.kvMap = make(map[string]string)
	kv.commitCallback = make(map[int][]commitCallbackInfo)
	kv.clientACK = make(map[int64]int64)
	kv.startApplyWorker()

	return kv
}

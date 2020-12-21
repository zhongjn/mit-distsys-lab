package raftkv

import (
	"labgob"
	"labrpc"
	"log"
	"raft"
	"time"
	"util"
)

// Debug print?
const Debug = 1

// DPrintf prints debug information
func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

// Op is a log entry in raft
type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Op        string // Put, Append, Get
	Key       string
	Value     string
	ClientID  int64
	RequestID int64
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
	kvMap          map[string]string            // key-value map
	commitCallback map[int][]commitCallbackInfo // commit index -> corresponding callback
	clientACK      map[int64]int64              // client ID -> the highest ACKed request ID
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

func (kv *KVServer) raftExecute(op Op, commitCallback func()) (isleader bool, committed bool) {
	kv.mu.Lock()

	if kv.killed {
		kv.mu.Unlock()
		return false, false
	}

	kv.mu.Unlock()

	index, term, isleader := kv.rf.Start(op)
	if !isleader {
		return false, false
	}

	kv.mu.Lock()
	DPrintf("Server #%d: leader executing op %v", kv.me, op)

	timeout := false
	ch := make(chan struct{})
	kv.registerCommitCallback(index, term, func(success bool) {
		kv.mu.AssertHeld()
		if success && !timeout {
			if commitCallback != nil {
				commitCallback()
			}
			ch <- struct{}{}
		} else {
			close(ch)
		}
	})

	kv.mu.Unlock()

	select {
	case _, ok := <-ch:
		return true, ok
	case <-time.After(200 * time.Millisecond):
		kv.mu.Lock()
		timeout = true
		kv.mu.Unlock()

		return false, false
	}
}

// Get RPC call
func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	// DPrintf("Server #%d: Get %s", kv.me, args.Key)

	var value string
	isleader, committed := kv.raftExecute(
		Op{
			Op:        "Read",
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

	isleader, committed := kv.raftExecute(Op{
		Op:        args.Op,
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
	kv.rf.Kill()

	// Your code here, if desired.
	kv.killCh <- struct{}{}

	kv.mu.Lock()
	kv.killed = true
	kv.mu.Unlock()
}

func (kv *KVServer) apply(op Op) (wrongRequestID bool) {
	kv.mu.AssertHeld()

	ack := kv.clientACK[op.ClientID]
	if ack >= op.RequestID {
		return false
	}

	if ack+1 != op.RequestID {
		return true
	}

	kv.clientACK[op.ClientID] = op.RequestID

	switch op.Op {
	case "Read":
		break
	case "Put":
		kv.kvMap[op.Key] = op.Value
		break
	case "Append":
		kv.kvMap[op.Key] += op.Value
		break
	default:
		log.Panicf("not supported op %s", op.Op)
	}

	return false
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
					op := msg.Command.(Op)
					// apply the message
					wrongRequestID := kv.apply(op)
					// execute commit callback if exist
					if cbs, ok := kv.commitCallback[msg.CommandIndex]; ok {
						delete(kv.commitCallback, msg.CommandIndex)
						for _, cb := range cbs {
							cb.callback(cb.term == msg.CommandTerm && !wrongRequestID)
						}
					}
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
	labgob.Register(Op{})

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

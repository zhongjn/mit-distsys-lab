package raftkv

import (
	"labgob"
	"labrpc"
	"log"
	"raft"
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

type state struct {
	kvMap     map[string]string // key-value map
	clientACK map[int64]int64   // client ID -> the highest ACKed request ID
}

type applyResult struct {
	requestIDTooHigh bool
	readResult       string
}

func defaultState() state {
	return state{
		kvMap:     make(map[string]string),
		clientACK: make(map[int64]int64),
	}
}

func applyFn(oldState interface{}, op interface{}) (newState interface{}, result interface{}) {
	s := oldState.(state)
	o := op.(opEntry)

	ack := s.clientACK[o.ClientID]

	if ack+1 < o.RequestID {
		return s, applyResult{requestIDTooHigh: true}
	}

	var readResult string

	switch o.Type {
	case "Read":
		readResult = s.kvMap[o.Key]
	case "Put":
		if ack < o.RequestID {
			s.kvMap[o.Key] = o.Value
		}
	case "Append":
		if ack < o.RequestID {
			s.kvMap[o.Key] += o.Value
		}
	default:
		log.Panicf("not supported op %s", o.Type)
		return nil, nil // unreachable
	}

	if ack < o.RequestID {
		s.clientACK[o.ClientID] = o.RequestID
	}
	return s, applyResult{readResult: readResult}
}

func snapshotFn(st interface{}) (snap interface{}) {
	s := st.(state)
	return snapshot{
		KVMap:     cloneMapStringString(s.kvMap),
		ClientACK: cloneMapInt64Int64(s.clientACK),
	}
}

func recoverFn(snap interface{}) (st interface{}) {
	s := snap.(snapshot)
	return state{
		kvMap:     cloneMapStringString(s.KVMap),
		clientACK: cloneMapInt64Int64(s.ClientACK),
	}
}

// KVServer provides RPC service
type KVServer struct {
	me int

	state state
	rf    *raft.Raft
	rh    *raft.Helper
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

// Get RPC call
func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	// DPrintf("Server #%d: Get %s", kv.me, args.Key)

	isleader, committed, result := kv.rh.Execute(
		opEntry{
			Type:      "Read",
			Key:       args.Key,
			ClientID:  args.ClientID,
			RequestID: args.RequestID,
		})

	if !isleader {
		*reply = GetReply{WrongLeader: true}
		return
	}

	if !committed {
		*reply = GetReply{Err: "commit failed"}
		return
	}

	res := result.(applyResult)
	if res.requestIDTooHigh {
		*reply = GetReply{Err: "request ID too high"}
		return
	}

	*reply = GetReply{Value: res.readResult}
}

// PutAppend RPC call
func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.

	if !(args.Op == "Put" || args.Op == "Append") {
		*reply = PutAppendReply{Err: "invalid op name"}
		return
	}

	// DPrintf("Server #%d: %s %s %s", kv.me, args.Op, args.Key, args.Value)

	isleader, committed, result := kv.rh.Execute(opEntry{
		Type:      args.Op,
		Key:       args.Key,
		Value:     args.Value,
		ClientID:  args.ClientID,
		RequestID: args.RequestID,
	})

	if !isleader {
		*reply = PutAppendReply{WrongLeader: true}
		return
	}

	if !committed {
		*reply = PutAppendReply{Err: "commit failed"}
		return
	}

	res := result.(applyResult)
	if res.requestIDTooHigh {
		*reply = PutAppendReply{Err: "request ID too high"}
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
	kv.rh.Kill()
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

	// You may need initialization code here.

	applyCh := make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, applyCh)
	kv.rh = raft.MakeHelper(kv.rf, applyCh, me, maxraftstate, defaultState(),
		applyFn, snapshotFn, recoverFn)

	return kv
}

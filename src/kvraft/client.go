package raftkv

import (
	"crypto/rand"
	"labrpc"
	"math/big"
	"util"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.

	mu            util.Mutex
	currentLeader int
	clientID      int64
	requestID     int64
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.clientID = nrand()
	return ck
}

func (ck *Clerk) getLeader() int {
	ck.mu.Lock()
	defer ck.mu.Unlock()

	return ck.currentLeader
}

func (ck *Clerk) getRequestID() int64 {
	ck.mu.Lock()
	defer ck.mu.Unlock()
	ck.requestID++
	return ck.requestID
}

func (ck *Clerk) wrongLeader(previous int) {
	ck.mu.Lock()
	defer ck.mu.Unlock()

	if ck.currentLeader == previous {
		ck.currentLeader = (ck.currentLeader + 1) % len(ck.servers)
	}

	DPrintf("Client #%d: changing leader from %d to %d", ck.clientID%100, previous, ck.currentLeader)
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	reqID := ck.getRequestID()

	// You will have to modify this function.
	for {
		leader := ck.getLeader()

		args := GetArgs{
			Key:       key,
			ClientID:  ck.clientID,
			RequestID: reqID,
		}

		DPrintf("Client #%d: Get leader=%d, args=%+v", ck.clientID%100, leader, args)
		var reply GetReply
		ok := ck.servers[leader].Call("KVServer.Get", &args, &reply)
		DPrintf("Client #%d: Get leader=%d, reply=%+v", ck.clientID%100, leader, reply)

		if ok {
			if reply.WrongLeader {
				ck.wrongLeader(leader)
			} else if reply.Err != "" {
				DPrintf("Get error: %s", reply.Err)
			} else {
				return reply.Value
			}
		} else {
			ck.wrongLeader(leader)
		}
	}
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	reqID := ck.getRequestID()

	// You will have to modify this function.
	for {
		leader := ck.getLeader()

		args := PutAppendArgs{
			Key:       key,
			Value:     value,
			Op:        op,
			ClientID:  ck.clientID,
			RequestID: reqID,
		}

		DPrintf("Client #%d: PutAppend leader=%d, args=%+v", ck.clientID%100, leader, args)
		var reply PutAppendReply
		ok := ck.servers[leader].Call("KVServer.PutAppend", &args, &reply)
		DPrintf("Client #%d: PutAppend leader=%d, reply=%+v", ck.clientID%100, leader, reply)

		if ok {
			if reply.WrongLeader {
				ck.wrongLeader(leader)
			} else if reply.Err != "" {
				DPrintf("PutAppend error: %s", reply.Err)
			} else {
				break
			}
		} else {
			ck.wrongLeader(leader)
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}

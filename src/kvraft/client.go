package raftkv

import (
	"crypto/rand"
	"labrpc"
	"log"
	"math/big"
	"time"
	"util"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.

	mu            util.Mutex
	currentLeader int
	// TODO: unique id to de-duplicate
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
	return ck
}

func (ck *Clerk) getLeader() int {
	ck.mu.Lock()
	defer ck.mu.Unlock()

	return ck.currentLeader
}

func (ck *Clerk) wrongLeader(previous int) {
	ck.mu.Lock()
	defer ck.mu.Unlock()

	if ck.currentLeader == previous {
		ck.currentLeader = (ck.currentLeader + 1) % len(ck.servers)
		log.Printf("Client: changing leader from %d to %d", previous, ck.currentLeader)
	}
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
	// You will have to modify this function.
	for {
		leader := ck.getLeader()

		args := GetArgs{Key: key}
		var reply GetReply
		ok := ck.servers[leader].Call("KVServer.Get", &args, &reply)

		if ok {
			if reply.WrongLeader {
				ck.wrongLeader(leader)
			} else if reply.Err != "" {
				log.Printf("Get error: %s", reply.Err)
			} else {
				return reply.Value
			}
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
	// You will have to modify this function.
	for {
		leader := ck.getLeader()

		args := PutAppendArgs{
			Key:   key,
			Value: value,
			Op:    op,
		}
		var reply PutAppendReply
		ok := ck.servers[leader].Call("KVServer.PutAppend", &args, &reply)

		if ok {
			if reply.WrongLeader {
				ck.wrongLeader(leader)
			} else if reply.Err != "" {
				log.Printf("PutAppend error: %s", reply.Err)
			} else {
				break
			}
		}

		time.Sleep(time.Millisecond * 100)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}

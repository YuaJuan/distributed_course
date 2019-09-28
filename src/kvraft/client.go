package raftkv

import (
	"crypto/rand"
	"labrpc"
	"math/big"
	"time"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	clientSeq      int
	me             int
	considerLeader int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

var ClerkID = 0

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {

	ck := new(Clerk)
	ck.servers = servers
	ck.clientSeq = 0
	ck.me = ClerkID
	ClerkID++
	DPrintf("MakeClerk %v", ClerkID)
	// You'll have to add code here.
	return ck
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
	args := GetArgs{
		Key:      key,
		ClientID: ck.me,
		Seq:      ck.clientSeq,
	}
	ck.clientSeq += 1
	var reply GetReply
	//serverNum := len(ck.servers)
	// You will have to modify this function.
	//这里用go要怎么写呢，怎么知道再go里结果已经争取返回了？
	//如何知识rpc失败，应该要重新发RPC.怎么try-again
	i := ck.considerLeader
	for {
		if ok := ck.servers[i].Call("KVServer.Get", &args, &reply); ok && reply.WrongLeader {
			ck.considerLeader = i
			//如果是超时，那么继续向该Leader发送请求
			if reply.Err == TimeOut {
				continue
			}
			return reply.Value
		}
		time.Sleep(time.Millisecond * 100)
		//如果不是leader或者rpc失败，则切换server
		i = (i + 1) % len(ck.servers)

	}
	return ""
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
	args := PutAppendArgs{
		Key:      key,
		Value:    value,
		Op:       op,
		ClientID: ck.me,
		Seq:      ck.clientSeq,
	}
	ck.clientSeq += 1
	//serverNum := len(ck.servers)
	var reply PutAppendReply
	i := ck.considerLeader
	for {
		if ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply); ok && reply.WrongLeader {
			ck.considerLeader = i
			//如果是超时，那么继续向该Leader发送请求
			if reply.Err == TimeOut {
				continue
			}
			return
		}
		time.Sleep(time.Millisecond * 100)
		//如果不是leader或者rpc失败，则切换server
		i = (i + 1) % len(ck.servers)

	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}

package shardmaster

//
// Shardmaster clerk.
//

import (
	"crypto/rand"
	"labrpc"
	"math/big"
	"sync/atomic"
	"time"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	clientSeq      int32
	me             int
	considerLeader int
}

var ClerkID = 0

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// Your code here.
	ck.clientSeq = 0
	ck.me = ClerkID
	ClerkID++

	return ck
}

func (ck *Clerk) Query(num int) Config {
	args := &QueryArgs{}
	// Your code here.
	atomic.AddInt32(&ck.clientSeq, 1)
	args.Seq = ck.clientSeq
	args.ClientId = ck.me
	args.Num = num
	for {
		// try each known server.
		for i, srv := range ck.servers {
			DPrintf("now try server %v", i)
			var reply QueryReply
			ok := srv.Call("ShardMaster.Query", args, &reply)
			if ok && reply.WrongLeader == true {
				return reply.Config
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	args := &JoinArgs{}
	// Your code here.
	atomic.AddInt32(&ck.clientSeq, 1)
	args.Seq = ck.clientSeq
	args.ClientId = ck.me
	args.Servers = servers

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply JoinReply
			ok := srv.Call("ShardMaster.Join", args, &reply)
			if ok && reply.WrongLeader == true {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Leave(gids []int) {
	args := &LeaveArgs{}
	// Your code here.
	args.GIDs = gids
	atomic.AddInt32(&ck.clientSeq, 1)
	args.Seq = ck.clientSeq
	args.ClientId = ck.me

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply LeaveReply
			ok := srv.Call("ShardMaster.Leave", args, &reply)
			if ok && reply.WrongLeader == true {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	args := &MoveArgs{}
	// Your code here.
	args.Shard = shard
	args.GID = gid
	atomic.AddInt32(&ck.clientSeq, 1)
	args.Seq = ck.clientSeq
	args.ClientId = ck.me
	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply MoveReply
			ok := srv.Call("ShardMaster.Move", args, &reply)
			if ok && reply.WrongLeader == true {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

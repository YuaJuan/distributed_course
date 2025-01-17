package shardkv

//
// Sharded key/value server.
// Lots of replica groups, each running op-at-a-time paxos.
// Shardmaster decides which group serves each shard.
// Shardmaster may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK            = "OK"
	ErrNoKey      = "ErrNoKey"
	ErrWrongGroup = "ErrWrongGroup"
)

const (
	NoKey   = Err("nokey")
	TimeOut = Err("timeout")
)

const BASE = 1000

const (
	GetOp    = "Get"
	PutOp    = "Put"
	AppendOp = "Append"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientID int
	Seq      int32
}

type PutAppendReply struct {
	WrongLeader bool
	Err         Err
}

type GetArgs struct {
	Key      string
	ClientID int
	Seq      int32

	// You'll have to add definitions here.
}

type GetReply struct {
	WrongLeader bool
	Err         Err
	Value       string
}

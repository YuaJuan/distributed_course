package raftkv

const (
	OK       = "OK"
	ErrNoKey = "ErrNoKey"
)

type Err string

const (
	NoKey   = Err("nokey")
	TimeOut = Err("timeout")
)

// Put or Append
type PutAppendArgs struct {
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
	ErrInfo     Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	ClientID int
	Seq      int32
}

type GetReply struct {
	WrongLeader bool
	ErrInfo     Err
	Value       string
}

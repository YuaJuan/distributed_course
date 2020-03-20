package shardkv

// import "shardmaster"
import (
	"labgob"
	"labrpc"
	"log"
	"raft"

	"shardmaster"
	"sync"
	"time"
)

const Debug = 1
const InfoPrint = 1

func InfoPrintf(format string, a ...interface{}) (n int, err error) {
	if InfoPrint > 0 {
		log.Printf(format, a...)
	}
	return
}
func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	OpName   string
	Key      string
	Value    string
	ClientId int
	Seq      int32
}

//Add code to server.go to periodically fetch the latest configuration from the shardmaster,
//and add code to reject client requests if the receiving group isn't responsible for the client's key's shard.
//You should still pass the first test.

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	masters      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big
	seqRecord    map[int]int32
	resCh        map[int]chan Op
	database     map[string]string
	mck          *shardmaster.Clerk
	shardServer  []int //该shardkv应该要服务的分片

	// Your definitions here.
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	op := Op{
		OpName:   GetOp,
		Key:      args.Key,
		ClientId: args.ClientID,
		Seq:      args.Seq,
	}

	isLeader, res, err := kv.StartCommand(op)
	reply.WrongLeader = isLeader
	reply.Value = res
	reply.Err = err
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.

	// put 先查找到key，再取代原来的值
	// Append 先查找到key,再追加值到后面，如果找不到key，直接新进入一条
	op := Op{
		OpName:   args.Op,
		Key:      args.Key,
		Value:    args.Value,
		ClientId: args.ClientID,
		Seq:      args.Seq,
	}
	isLeader, _, err := kv.StartCommand(op)
	reply.WrongLeader = isLeader
	reply.Err = err
}

//checkDup
//return true, if the seq has before
func (kv *ShardKV) checkDup(op Op) bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if seq, ok := kv.seqRecord[op.ClientId]; ok {

		if seq >= op.Seq {
			return true
		}
	}
	return false

}

func (kv *ShardKV) StartCommand(op Op) (bool, string, Err) {
	DPrintf("shardkv %v start command name %v,key %v,value %v,clientId %v,seq %v.", kv.me, op.OpName, op.Key, op.Value, op.ClientId, op.Seq)

	//如何之前该请求已经执行了，则直接返回结果
	if kv.checkDup(op) {
		DPrintf("clientid %v have dup name %v key %v value %v seq %v", op.ClientId, op.OpName, op.Key, op.Value, op.Seq)
		if op.OpName == GetOp {
			//DPrintf("dup client %v server %v Get %v found value %v database %v", op.ClientId, kv.me, op.Key, kv.database[op.Key], kv.database)
			kv.mu.Lock()
			if val, ok := kv.database[op.Key]; ok {
				kv.mu.Unlock()
				return true, val, OK
			}
			kv.mu.Unlock()
			return true, "", NoKey
		} else {
			return true, "", OK
		}
	}
	//问题在这儿，这儿就没有返回@！！！！
	_, _, isLeader := kv.rf.Start(op)

	if !isLeader {
		DPrintf("server %v is not leader ,so return .", kv.me)
		return false, "", Err("")
	}
	DPrintf("server %v is leader.", kv.me)
	//kv.mu.Lock()
	kv.resCh[op.ClientId*BASE+int(op.Seq)] = make(chan Op, 1)
	applyCh := kv.resCh[op.ClientId*BASE+int(op.Seq)]
	//kv.mu.Unlock()
	select {
	case <-applyCh:
		//TODO:sync map
		InfoPrintf("%v kv %v startcommand receive name %v,key %v,value %v,clientId %v,seq %v.", op.ClientId*BASE+int(op.Seq), kv.me, op.OpName, op.Key, op.Value, op.ClientId, op.Seq)
		if op.OpName == GetOp {
			kv.mu.Lock()
			if val, ok := kv.database[op.Key]; ok {
				DPrintf("client %v server %v Get %v found value %v database %v ", op.ClientId, kv.me, op.Key, val, kv.database)
				kv.mu.Unlock()
				return true, val, OK
			}
			DPrintf("Get %v not found ", op.Key)
			kv.mu.Unlock()
			return true, "", NoKey
		} else if op.OpName == PutOp {
			return true, "", OK
		} else {
			return true, "", OK
		}

	case <-time.After(time.Millisecond * 3000):
		InfoPrintf("time out when execute opname %v, clientid %v, seq %v,key %v", op.OpName, op.ClientId, op.Seq, op.Key)
		return isLeader, "", TimeOut

	}

}

func (kv *ShardKV) acceptMsg() {
	go func() {
		for {
			applymsg := <-kv.applyCh
			if !applymsg.CommandValid {
				InfoPrintf("commmand is not valid !")
				continue
			}
			if op, ok := applymsg.Command.(Op); ok {
				//确认command已经执行后，再记录
				seq := op.Seq
				clientId := op.ClientId
				if kv.checkDup(op) {
					DPrintf("seq %v client id %v has executed before.", op.Seq, op.ClientId)
					continue
				}
				kv.mu.Lock()
				DPrintf("kvserve %v, applymsg seq %v,clientid %v,opname %v,key %v", kv.me, seq, clientId, op.OpName, op.Key)
				kv.seqRecord[clientId] = seq
				switch op.OpName {
				case AppendOp:
					kv.database[op.Key] += op.Value
				case PutOp:
					DPrintf("put value %v database %v ", op.Value, kv.database)
					kv.database[op.Key] = op.Value
				}

				//这里之前没有处理，导致不是leader的进程一直阻塞在这里（因为没有进程来读取ch channel)
				if ch, ok := kv.resCh[clientId*BASE+int(seq)]; ok {
					select {
					case <-ch:
					default:
					}
					ch <- op
				}
				//DPrintf("%v after", kv.me)
				kv.mu.Unlock()
			} else {
				InfoPrintf("command not a op type.")
			}

		}
	}()
}

//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardmaster.
//
// pass masters[] to shardmaster.MakeClerk() so you can send
// RPCs to the shardmaster.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use masters[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, masters []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.masters = masters
	kv.seqRecord = make(map[int]int32)
	kv.resCh = make(map[int]chan Op)
	kv.database = make(map[string]string)

	// Your initialization code here.

	// Use something like this to talk to the shardmaster:
	//kv.mck = shardmaster.MakeClerk(kv.masters)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	return kv
}

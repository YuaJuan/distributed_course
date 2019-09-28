package raftkv

import (
	"labgob"
	"labrpc"
	"log"
	"raft"
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
	Seq      int
}

//1. 修改raft.ApplyMsg
//2. 修改AppendEntries
type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	seqRecord map[int]int
	resCh     map[int]chan Op

	maxraftstate int // snapshot if log grows this big
	database     map[string]string

	// Your definitions here.
}

//开始把字母大写弄错了
const (
	GetOp    = "Get"
	PutOp    = "Put"
	AppendOp = "Append"
)

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
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

// 客户端提交指令后，要等待该指令被leader提交才能返回结果
// 在applymsg中来标识是哪一条指令吗

// 客户端启动后，随机连接一个server,如果server不是leader,则会拒绝request,但是会返回客户端他所知道的leader地址
// AppendEntries里包含了客户端的地址

// 如何避免同一客户端的同一指令被多次运行
// 客户端为指令维护一个序列号，如何server检测到同一客户端的相同序列号已经被运行过，则拒绝再次运行，直接返回结果

// read-only操作如何避免获得stale data
// 1. 每个leader提交一次no op操作 ？？？
// 2.

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
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
	//这里的结果应该怎么得到呢？raft中的提交是通过applyMsg返回的
}

//checkDup
//return true, if the seq has before
func (kv *KVServer) checkDup(op Op) bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if seq, ok := kv.seqRecord[op.ClientId]; ok {
		//DPrintf("clientid %v oldseq %v curseq %v", op.ClientId, kv.seqRecord[op.ClientId], seq)
		if seq >= op.Seq {
			return true
		}
	}
	return false

}

func (kv *KVServer) StartCommand(op Op) (bool, string, Err) {
	//	InfoPrintf("%v start command name %v,key %v,value %v,clientId %v,seq %v.", kv.me, op.OpName, op.Key, op.Value, op.ClientId, op.Seq)

	//如何之前该请求已经执行了，则直接返回结果
	if kv.checkDup(op) {
		if op.OpName == GetOp {
			return true, kv.database[op.Key], Err("")
		} else {
			return true, "", Err("")
		}
	}
	_, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		//InfoPrintf("not leader....")
		return false, "", Err("")
	}
	kv.mu.Lock()
	kv.resCh[op.ClientId*BASE+op.Seq] = make(chan Op, 1)
	applyCh := kv.resCh[op.ClientId*BASE+op.Seq]
	kv.mu.Unlock()
	select {
	case <-applyCh:
		kv.mu.Lock()
		InfoPrintf("%v kv %v startcommand receive name %v,key %v,value %v,clientId %v,seq %v.", op.ClientId*BASE+op.Seq, kv.me, op.OpName, op.Key, op.Value, op.ClientId, op.Seq)
		if op.OpName == GetOp {
			if val, ok := kv.database[op.Key]; ok {
				kv.mu.Unlock()
				return true, val, Err("")
			}
			kv.mu.Unlock()
			return true, "", NoKey
		} else if op.OpName == PutOp {
			kv.database[op.Key] = op.Value
			//	DPrintf("Append key %v value %v", op.Key, op.Value)
			//	DPrintf("after append:%v", kv.database[op.Key])ß
			kv.mu.Unlock()
			return true, "", Err("")
		} else {
			if _, ok := kv.database[op.Key]; ok {
				kv.database[op.Key] += op.Value
			} else {
				kv.database[op.Key] = op.Value
			}
			//DPrintf("Append key %v value %v", op.Key, op.Value)
			//DPrintf("after append:%v", kv.database[op.Key])
			kv.mu.Unlock()
			return true, "", Err("")
		}

	case <-time.After(time.Millisecond * 3000):
		InfoPrintf("time out when execute opname %v, clientid %v, seq %v,key %v", op.OpName, op.ClientId, op.Seq, op.Key)
		return isLeader, "", TimeOut
	}
	return isLeader, "", Err("")
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *KVServer) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

//
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

	InfoPrintf("StartKVServer.......")
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.database = make(map[string]string)
	kv.resCh = make(map[int]chan Op)
	kv.seqRecord = make(map[int]int)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	//在这里循环读入applyC
	kv.acceptMsg()
	// You may need initialization code here.
	return kv
}

const BASE = 1000

func (kv *KVServer) acceptMsg() {
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
				//DPrintf("kvserve %v wrtie to %v", kv.me, clientId*BASE+seq)

				//这里之前没有处理，导致不是leader的进程一直阻塞在这里（因为没有进程来读取channel)
				if ch, ok := kv.resCh[clientId*BASE+seq]; ok {
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

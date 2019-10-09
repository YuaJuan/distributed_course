package shardmaster

import (
	"labgob"
	"labrpc"
	"log"
	"raft"
	"sync"
	"time"
)

const Debug = 0
const InfoPrint = 0

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

const BASE = 1000

const (
	Timeout    = "timeout"
	OpNameErr  = "op name error"
	CommandErr = "command error"
)

type ShardMaster struct {
	mu        sync.Mutex
	me        int
	rf        *raft.Raft
	applyCh   chan raft.ApplyMsg
	resCh     map[int]chan Op
	seqRecord map[int]int32
	// Your data here.
	configID int
	configs  []Config // indexed by config num
}

type Op struct {
	// Your data here.
	OpName   string
	ClientId int
	Seq      int32

	//args of command Join/Move/Leave/Query need
	Servers map[int][]string //Join  new GID -> servers mappings
	GIDs    []int            //Leave
	Shard   int              //Move
	Gid     int              //Move  Move shard to gid
	Num     int              //Query the num of sm.configs
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	servers := make(map[int][]string)
	for gid, sers := range args.Servers {
		servers[gid] = make([]string, len(sers))
		copy(servers[gid], sers)
	}
	op := Op{
		OpName:   "Join",
		ClientId: args.ClientId,
		Seq:      args.Seq,
		Servers:  servers,
	}
	isLeader, err := sm.StartCommand(op)
	reply.WrongLeader = isLeader
	reply.Err = err
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	gids := make([]int, len(args.GIDs))
	copy(gids, args.GIDs)
	op := Op{
		OpName:   "Leave",
		ClientId: args.ClientId,
		Seq:      args.Seq,
		GIDs:     gids,
	}
	isLeader, err := sm.StartCommand(op)
	reply.WrongLeader = isLeader
	reply.Err = err
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	op := Op{
		OpName:   "Move",
		ClientId: args.ClientId,
		Seq:      args.Seq,
		Shard:    args.Shard,
		Gid:      args.GID,
	}
	isLeader, err := sm.StartCommand(op)
	reply.WrongLeader = isLeader
	reply.Err = err
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	op := Op{
		OpName:   "Query",
		ClientId: args.ClientId,
		Seq:      args.Seq,
		Num:      args.Num,
	}
	isLeader, err := sm.StartCommand(op)
	reply.WrongLeader = isLeader
	reply.Err = err
	if isLeader == true && err == Err("") {
		sm.mu.Lock()
		num := op.Num
		if num >= len(sm.configs) || num < 0 {
			num = len(sm.configs) - 1
		}
		sm.mu.Unlock()
		reply.Config = sm.configs[num]
		DPrintf("Query %v reply.Config %v", num, reply.Config)
	}
}

//
// the tester calls Kill() when a ShardMaster instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sm *ShardMaster) Kill() {
	sm.rf.Kill()
	// Your code here, if desired.
}

// needed by shardsm tester
func (sm *ShardMaster) Raft() *raft.Raft {
	return sm.rf
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardMaster {

	sm := new(ShardMaster)
	sm.me = me
	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sm.applyCh = make(chan raft.ApplyMsg)
	sm.rf = raft.Make(servers, me, persister, sm.applyCh)
	sm.seqRecord = make(map[int]int32)
	sm.resCh = make(map[int]chan Op)
	//init a empty config
	sm.InitSmConfigs()
	// Your code here.
	sm.acceptMsg()

	return sm
}

func (sm *ShardMaster) checkDup(op Op) bool {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	if seq, ok := sm.seqRecord[op.ClientId]; ok {
		if seq >= op.Seq {
			return true
		}
	}
	return false
}

func (sm *ShardMaster) InitSmConfigs() Config {
	res := Config{}
	res.Num = 0
	for i := 0; i < NShards; i++ {
		res.Shards[i] = 0
	}
	res.Groups = make(map[int][]string)
	return res
}

func (sm *ShardMaster) StartCommand(op Op) (bool, Err) {
	DPrintf("start command name %v command", op.OpName)

	//如何之前该请求已经执行了，则直接返回结果
	if sm.checkDup(op) {
		DPrintf("%v have executed before.", op.OpName)
		return true, Err("")
	}

	_, _, isLeader := sm.rf.Start(op)

	if !isLeader {
		return isLeader, Err("")
	}
	sm.mu.Lock()
	sm.resCh[op.ClientId*BASE+int(op.Seq)] = make(chan Op, 1)
	applyCh := sm.resCh[op.ClientId*BASE+int(op.Seq)]
	sm.mu.Unlock()
	select {
	case <-applyCh:
		return true, Err("")
	case <-time.After(time.Millisecond * 3000):
		return isLeader, Timeout
	}
	//return isLeader, Err("")
}

func (sm *ShardMaster) reBalance(cfg *Config, opname string, gid int) {
	// sm.mu.Lock()
	// defer sm.mu.Unlock()
	gidShards := sm.shardByGid(cfg)
	switch opname {
	case "Join":
		if len(cfg.Groups) == 1 {
			for s, _ := range cfg.Shards {
				cfg.Shards[s] = gid
			}
		}

		avg := NShards / len(cfg.Groups)
		//TODO:更加高效
		for i := 0; i < avg; i++ {
			maxShardGid := sm.maxShardOfGid(gidShards)
			cfg.Shards[gidShards[maxShardGid][0]] = gid
			gidShards[maxShardGid] = gidShards[maxShardGid][1:]
		}
	case "Leave":
		shards, _ := gidShards[gid]
		delete(cfg.Groups, gid)
		delete(gidShards, gid)
		for _, shard := range shards {
			minShardGid := sm.minShardOfGid(gidShards)
			cfg.Shards[shard] = minShardGid
			gidShards[minShardGid] = append(gidShards[minShardGid], shard)
		}
	}

}

func (sm *ShardMaster) maxShardOfGid(gidShards map[int][]int) int {
	shardnum := 0
	mostGid := 0
	for gid, shards := range gidShards {
		if len(shards) > shardnum {
			shardnum = len(shards)
			mostGid = gid
		}
	}
	return mostGid
}

func (sm *ShardMaster) minShardOfGid(gidShards map[int][]int) int {
	shardnum := 10000000
	leastGid := 0
	for gid, shards := range gidShards {
		if len(shards) < shardnum {
			shardnum = len(shards)
			leastGid = gid
		}
	}
	return leastGid
}

func (sm *ShardMaster) shardByGid(cfg *Config) map[int][]int {
	gidShards := make(map[int][]int)
	for v, _ := range cfg.Groups {
		gidShards[v] = []int{}
	}
	for shard, gid := range cfg.Shards {
		gidShards[gid] = append(gidShards[gid], shard)
	}
	return gidShards
}

func (sm *ShardMaster) updateConfig(op *Op) {
	DPrintf("%v before updateconfig %v ", op.OpName, sm.configs)
	newconfig := sm.createNextConfig()
	//err := Err("")
	switch op.OpName {
	case "Join":

		for gid, servers := range op.Servers {
			newServers := make([]string, len(servers))
			copy(newServers, servers)
			newconfig.Groups[gid] = newServers
			sm.reBalance(&newconfig, op.OpName, gid)
		}

	case "Leave":
		for _, gid := range op.GIDs {
			if _, exist := newconfig.Groups[gid]; exist {
				sm.reBalance(&newconfig, op.OpName, gid)
			}
		}

	case "Move":
		if _, exist := newconfig.Groups[op.Gid]; exist {
			newconfig.Shards[op.Shard] = op.Gid
		}

	}
	sm.configs = append(sm.configs, newconfig)
	DPrintf("%v after updateconfig %v me %v ", op.OpName, sm.configs, sm.me)
	//return err
}

func (sm *ShardMaster) createNextConfig() Config {
	lastCfg := sm.configs[len(sm.configs)-1]
	nextCfg := Config{
		Num:    lastCfg.Num + 1,
		Shards: lastCfg.Shards,
		Groups: make(map[int][]string),
	}
	for gid, servers := range lastCfg.Groups {
		nextCfg.Groups[gid] = append([]string{}, servers...)
	}
	return nextCfg
}

func (sm *ShardMaster) acceptMsg() {
	go func() {
		for {
			applymsg := <-sm.applyCh
			if !applymsg.CommandValid {
				InfoPrintf("commmand is not valid !")
				continue
			}
			DPrintf("server %v accpet apply %v", sm.me, applymsg.Command)
			if op, ok := applymsg.Command.(Op); ok {
				//确认command已经执行后，再记录
				seq := op.Seq
				clientId := op.ClientId
				if sm.checkDup(op) {
					DPrintf("seq %v client id %v has executed before.", op.Seq, op.ClientId)
					continue
				}
				sm.mu.Lock()
				if op.OpName != "Query" {
					sm.updateConfig(&op)
				}
				sm.seqRecord[clientId] = seq
				if ch, ok := sm.resCh[clientId*BASE+int(seq)]; ok {
					select {
					case <-ch:
					default:
					}
					ch <- op
				}

				sm.mu.Unlock()
			} else {
				InfoPrintf("command not a op type.")
			}

		}
	}()
}

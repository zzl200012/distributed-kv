package shardmaster

import (
	"distributed-kv/labrpc"
	"distributed-kv/raft"
	"encoding/gob"
	"sync"
	"time"
)


type ShardMaster struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	configs         []Config
	latestSequences map[int64]int64           // for deduplication [clientId -> latest seq]
	notifyCh        map[int]chan RaftResponse // for notification [commandIndex -> chan RaftResponse]
}

type Op struct {
	Command     string
	ClientId    int64
	SequenceNum int64
	Servers     map[int][]string
	GIDs        []int
	Shard       int
	GID         int
	Num         int
}

// response from raft call
type RaftResponse struct {
	Command     string
	OK          bool
	ClientId    int64
	SequenceNum int64
	WrongLeader bool
	Err         Err
	Config      Config
}

func (sm *ShardMaster) applyToRaft(entry Op) RaftResponse {
	index, _, isLeader := sm.rf.Start(entry)
	if !isLeader {
		return RaftResponse{OK: false}
	}

	sm.mu.Lock()
	if _, ok := sm.notifyCh[index]; !ok {
		sm.notifyCh[index] = make(chan RaftResponse, 1)
	}
	sm.mu.Unlock()

	select {
	case result := <-sm.notifyCh[index]:
		if isMatch(entry, result) {
			return result
		}
		return RaftResponse{OK: false}
	case <-time.After(250 * time.Millisecond):
		return RaftResponse{OK: false}
	}
}

func isMatch(entry Op, result RaftResponse) bool {
	return entry.ClientId == result.ClientId && entry.SequenceNum == result.SequenceNum
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) {
	entry := Op{}
	entry.Command = "join"
	entry.ClientId = args.ClientId
	entry.SequenceNum = args.SequenceNum
	entry.Servers = args.Servers

	result := sm.applyToRaft(entry)
	if !result.OK {
		reply.WrongLeader = true
		return
	}
	reply.WrongLeader = false
	reply.Err = result.Err
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {
	entry := Op{}
	entry.Command = "leave"
	entry.ClientId = args.ClientId
	entry.SequenceNum = args.SequenceNum
	entry.GIDs = args.GIDs

	result := sm.applyToRaft(entry)
	if !result.OK {
		reply.WrongLeader = true
		return
	}
	reply.WrongLeader = false
	reply.Err = result.Err
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	entry := Op{}
	entry.Command = "move"
	entry.ClientId = args.ClientId
	entry.SequenceNum = args.SequenceNum
	entry.Shard = args.Shard
	entry.GID = args.GID

	result := sm.applyToRaft(entry)
	if !result.OK {
		reply.WrongLeader = true
		return
	}
	reply.WrongLeader = false
	reply.Err = result.Err
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
	entry := Op{}
	entry.Command = "query"
	entry.ClientId = args.ClientId
	entry.SequenceNum = args.SequenceNum
	entry.Num = args.Num

	result := sm.applyToRaft(entry)
	if !result.OK {
		reply.WrongLeader = true
		return
	}
	reply.WrongLeader = false
	reply.Err = result.Err
	reply.Config = result.Config
}

func (sm *ShardMaster) applyToLocal(op Op) RaftResponse {
	result := RaftResponse{}
	result.Command = op.Command
	result.OK = true
	result.WrongLeader = false
	result.ClientId = op.ClientId
	result.SequenceNum = op.SequenceNum

	switch op.Command {
	case "join":
		if !sm.isDuplicated(op) {
			sm.applyJoin(op)
		}
		result.Err = OK
	case "leave":
		if !sm.isDuplicated(op) {
			sm.applyLeave(op)
		}
		result.Err = OK
	case "move":
		if !sm.isDuplicated(op) {
			sm.applyMove(op)
		}
		result.Err = OK
	case "query":
		if op.Num == -1 || op.Num >= len(sm.configs) {
			result.Config = sm.configs[len(sm.configs)-1]
		} else {
			result.Config = sm.configs[op.Num]
		}
		result.Err = OK
	}
	sm.latestSequences[op.ClientId] = op.SequenceNum
	return result
}

func (sm *ShardMaster) isDuplicated(op Op) bool {
	lastSeq, ok := sm.latestSequences[op.ClientId]
	if ok {
		return lastSeq >= op.SequenceNum
	}
	return false
}

func (sm *ShardMaster) getNextConfig() Config {
	nextConfig := Config{}
	currConfig := sm.configs[len(sm.configs)-1]

	nextConfig.Num = currConfig.Num + 1
	nextConfig.Shards = currConfig.Shards
	nextConfig.Groups = map[int][]string{}
	for gid, servers := range currConfig.Groups {
		nextConfig.Groups[gid] = servers
	}
	return nextConfig
}

func (sm *ShardMaster) applyJoin(args Op) {
	config := sm.getNextConfig()
	for gid, servers := range args.Servers {
		config.Groups[gid] = servers

		for i := 0; i < NShards; i++ {
			if config.Shards[i] == 0 {
				config.Shards[i] = gid
			}
		}
	}
	reBalance(&config)
	sm.configs = append(sm.configs, config)
}

func (sm *ShardMaster) applyLeave(args Op) {
	config := sm.getNextConfig()
	stayG := 0
	for gid := range config.Groups {
		stay := true
		for _, deleted := range args.GIDs {
			if gid == deleted {
				stay = false
			}
		}
		if stay {
			stayG = gid
			break
		}
	}

	for _, gid := range args.GIDs {
		for i := 0; i < len(config.Shards); i++ {
			if config.Shards[i] == gid {
				config.Shards[i] = stayG
			}
		}
		delete(config.Groups, gid)
	}
	reBalance(&config)
	sm.configs = append(sm.configs, config)
}

func (sm *ShardMaster) applyMove(args Op) {
	config := sm.getNextConfig()
	config.Shards[args.Shard] = args.GID
	sm.configs = append(sm.configs, config)
}


func reBalance(config *Config) {
	g2s := g2s(config)
	if len(config.Groups) == 0 {
		// no replica group
		for i := 0; i < len(config.Shards); i++ {
			config.Shards[i] = 0
		}
	} else {
		avg := NShards / len(config.Groups)
		move := 0
		for _, shards := range g2s {
			if len(shards) > avg {
				move += len(shards) - avg
			}
		}
		for i := 0; i < move; i++ {
			srcGid, dstGid := getMaxAndMin(g2s)
			N := len(g2s[srcGid]) - 1
			config.Shards[g2s[srcGid][N]] = dstGid
			g2s[dstGid] = append(g2s[dstGid], g2s[srcGid][N])
			g2s[srcGid] = g2s[srcGid][:N]
		}
	}
}

func g2s(config *Config) map[int][]int {
	g2s := make(map[int][]int)
	for gid := range config.Groups {
		g2s[gid] = make([]int, 0)
	}
	for shard, gid := range config.Shards {
		g2s[gid] = append(g2s[gid], shard)
	}
	return g2s
}

func getMaxAndMin(g2s map[int][]int) (int, int) {
	srcGid, dstGid := 0, 0
	for gid, shards := range g2s {
		if srcGid == 0 || len(g2s[srcGid]) < len(shards) {
			srcGid = gid
		}
		if dstGid == 0 || len(g2s[dstGid]) > len(shards) {
			dstGid = gid
		}
	}
	return srcGid, dstGid
}

func (sm *ShardMaster) Kill() {
	sm.rf.Kill()
}

func (sm *ShardMaster) Raft() *raft.Raft {
	return sm.rf
}

func (sm *ShardMaster) Run() {
	for {
		msg := <-sm.applyCh
		sm.mu.Lock()

		op := msg.Command.(Op)
		res := sm.applyToLocal(op)
		if ch, ok := sm.notifyCh[msg.CommandIndex]; ok {
			select {
			case <-ch:
			default:
			}
		} else {
			sm.notifyCh[msg.CommandIndex] = make(chan RaftResponse, 1)
		}
		sm.notifyCh[msg.CommandIndex] <- res
		sm.mu.Unlock()
	}
}

func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardMaster {
	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int][]string{}

	gob.Register(Op{})
	gob.Register(RaftResponse{})

	sm.applyCh = make(chan raft.ApplyMsg, 100)
	sm.rf = raft.Make(servers, me, persister, sm.applyCh)

	sm.latestSequences = make(map[int64]int64)
	sm.notifyCh = make(map[int]chan RaftResponse)

	go sm.Run()
	return sm
}

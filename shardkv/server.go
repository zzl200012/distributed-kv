package shardkv

import (
	"bytes"
	"distributed-kv/labrpc"
	"distributed-kv/raft"
	"distributed-kv/shardmaster"
	"encoding/gob"
	"sync"
	"time"
)

type Op struct {
	Command     string
	ClientId    int64
	SequenceNum int64
	Key         string
	Value       string
	Config      shardmaster.Config
	Data        [shardmaster.NShards]map[string]string
	LatestSeq   map[int64]int64
	ConfigNum   int
	ShardId     int
}

type Result struct {
	Command     string
	OK          bool
	WrongLeader bool
	Err         Err
	ClientId    int64
	SequenceNum int64
	Value       string
	ConfigNum   int
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	makeEnd      func(string) *labrpc.ClientEnd
	gid          int
	masters      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	data            [shardmaster.NShards]map[string]string
	latestSequences map[int64]int64
	notifyCh        map[int]chan Result
	config          shardmaster.Config
	mck             *shardmaster.Clerk
}

//
// try to append the entry to raft servers' log and return result.
// result is valid if raft servers apply this entry before timeout.
//
func (kv *ShardKV) appendEntryToLog(entry Op) Result {
	index, _, isLeader := kv.rf.Start(entry)
	if !isLeader {
		return Result{OK: false}
	}

	kv.mu.Lock()
	if _, ok := kv.notifyCh[index]; !ok {
		kv.notifyCh[index] = make(chan Result, 1)
	}
	kv.mu.Unlock()

	select {
	case result := <-kv.notifyCh[index]:
		if isMatch(entry, result) {
			return result
		}
		return Result{OK: false}
	case <-time.After(250 * time.Millisecond):
		return Result{OK: false}
	}
}

//
// check if the result corresponds to the log entry.
//
func isMatch(entry Op, result Result) bool {
	if entry.Command == "reconfigure" {
		return entry.Config.Num == result.ConfigNum
	}
	if entry.Command == "delete" {
		return entry.ConfigNum == result.ConfigNum
	}
	return entry.ClientId == result.ClientId && entry.SequenceNum == result.SequenceNum
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	entry := Op{}
	entry.Command = "get"
	entry.ClientId = args.ClientId
	entry.SequenceNum = args.SequenceNum
	entry.Key = args.Key

	result := kv.appendEntryToLog(entry)
	if !result.OK {
		reply.WrongLeader = true
		return
	}
	reply.WrongLeader = false
	reply.Err = result.Err
	reply.Value = result.Value
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	entry := Op{}
	entry.Command = args.Op
	entry.ClientId = args.ClientId
	entry.SequenceNum = args.SequenceNum
	entry.Key = args.Key
	entry.Value = args.Value

	result := kv.appendEntryToLog(entry)
	if !result.OK {
		reply.WrongLeader = true
		return
	}
	reply.WrongLeader = false
	reply.Err = result.Err
}

func (kv *ShardKV) applyOp(op Op) Result {
	result := Result{}
	result.Command = op.Command
	result.OK = true
	result.WrongLeader = false
	result.ClientId = op.ClientId
	result.SequenceNum = op.SequenceNum

	switch op.Command {
	case "put":
		kv.applyPut(op, &result)
	case "append":
		kv.applyAppend(op, &result)
	case "get":
		kv.applyGet(op, &result)
	case "reconfigure":
		kv.applyReconfigure(op, &result)
	case "delete":
		kv.applyCleanup(op, &result)
	}
	return result
}

func (kv *ShardKV) applyPut(args Op, result *Result) {
	if !kv.isValidKey(args.Key) {
		result.Err = ErrWrongGroup
		return
	}
	if !kv.isDuplicated(args) {
		kv.data[key2shard(args.Key)][args.Key] = args.Value
		kv.latestSequences[args.ClientId] = args.SequenceNum
	}
	result.Err = OK
}

func (kv *ShardKV) applyAppend(args Op, result *Result) {
	if !kv.isValidKey(args.Key) {
		result.Err = ErrWrongGroup
		return
	}
	if !kv.isDuplicated(args) {
		kv.data[key2shard(args.Key)][args.Key] += args.Value
		kv.latestSequences[args.ClientId] = args.SequenceNum
	}
	result.Err = OK
}

func (kv *ShardKV) applyGet(args Op, result *Result) {
	if !kv.isValidKey(args.Key) {
		result.Err = ErrWrongGroup
		return
	}
	if !kv.isDuplicated(args) {
		kv.latestSequences[args.ClientId] = args.SequenceNum
	}
	if value, ok := kv.data[key2shard(args.Key)][args.Key]; ok {
		result.Value = value
		result.Err = OK
	} else {
		result.Err = ErrNoKey
	}
}

func (kv *ShardKV) applyReconfigure(args Op, result *Result) {
	result.ConfigNum = args.Config.Num
	if args.Config.Num == kv.config.Num+1 {
		for shardId, shardData := range args.Data {
			for k, v := range shardData {
				kv.data[shardId][k] = v
			}
		}
		// merge latestSequences map from args to this server's latestSequences map.
		for clientId := range args.LatestSeq {
			if _, ok := kv.latestSequences[clientId]; !ok || kv.latestSequences[clientId] < args.LatestSeq[clientId] {
				kv.latestSequences[clientId] = args.LatestSeq[clientId]
			}
		}

		lastConfig := kv.config
		kv.config = args.Config

		// ask other replica groups to delete shards no longer required.
		for shardId, shardData := range args.Data {
			if len(shardData) > 0 {
				gid := lastConfig.Shards[shardId]
				args := DeleteShardArgs{}
				args.Num = lastConfig.Num
				args.ShardId = shardId
				go kv.sendDeleteShard(gid, &lastConfig, &args, &DeleteShardReply{})
			}
		}
	}
	result.Err = OK
}

func (kv *ShardKV) applyCleanup(args Op, result *Result) {
	if args.ConfigNum <= kv.config.Num {
		if kv.gid != kv.config.Shards[args.ShardId] {
			kv.data[args.ShardId] = make(map[string]string)
		}
	}
}

func (kv *ShardKV) isDuplicated(op Op) bool {
	lastSeq, ok := kv.latestSequences[op.ClientId]
	if ok {
		return lastSeq >= op.SequenceNum
	}
	return false
}

func (kv *ShardKV) isValidKey(key string) bool {
	return kv.config.Shards[key2shard(key)] == kv.gid
}

func (kv *ShardKV) Kill() {
	kv.rf.Kill()
}

func (kv *ShardKV) Run() {
	for {
		msg := <-kv.applyCh
		kv.mu.Lock()
		if msg.UseSnapshot {
			r := bytes.NewBuffer(msg.Snapshot)
			d := gob.NewDecoder(r)

			var lastIncludedIndex, lastIncludedTerm int
			d.Decode(&lastIncludedIndex)
			d.Decode(&lastIncludedTerm)
			d.Decode(&kv.data)
			d.Decode(&kv.latestSequences)
			d.Decode(&kv.config)
		} else {
			// apply operation and send result
			op := msg.Command.(Op)
			result := kv.applyOp(op)
			if ch, ok := kv.notifyCh[msg.CommandIndex]; ok {
				select {
				case <-ch: // drain bad data
				default:
				}
			} else {
				kv.notifyCh[msg.CommandIndex] = make(chan Result, 1)
			}
			kv.notifyCh[msg.CommandIndex] <- result

			// create snapshot if raft state exceeds allowed size
			if kv.maxraftstate != -1 && kv.rf.GetRaftStateSize() > kv.maxraftstate {
				w := new(bytes.Buffer)
				e := gob.NewEncoder(w)
				e.Encode(kv.data)
				e.Encode(kv.latestSequences)
				e.Encode(kv.config)
				go kv.rf.TakeSnapshot(w.Bytes(), msg.CommandIndex)
			}
		}
		kv.mu.Unlock()
	}
}

type TransferShardArgs struct {
	Num      int
	ShardIds []int
}

type TransferShardReply struct {
	Err       Err
	Data      [shardmaster.NShards]map[string]string
	LatestSeq map[int64]int64
}

func (kv *ShardKV) TransferShard(args *TransferShardArgs, reply *TransferShardReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if kv.config.Num < args.Num {
		// this server is not ready (may still handle the requested shards).
		reply.Err = ErrNotReady
		return
	}

	// copy shards and latestSequences to reply.
	for i := 0; i < shardmaster.NShards; i++ {
		reply.Data[i] = make(map[string]string)
	}
	for _, shardId := range args.ShardIds {
		for k, v := range kv.data[shardId] {
			reply.Data[shardId][k] = v
		}
	}
	reply.LatestSeq = make(map[int64]int64)
	for clientId, requestId := range kv.latestSequences {
		reply.LatestSeq[clientId] = requestId
	}
	reply.Err = OK
}

//
// try to get shards requested in args and latestSequences from replica group specified by gid.
//
func (kv *ShardKV) sendTransferShard(gid int, args *TransferShardArgs, reply *TransferShardReply) bool {
	for _, server := range kv.config.Groups[gid] {
		srv := kv.makeEnd(server)
		ok := srv.Call("ShardKV.TransferShard", args, reply)
		if ok {
			if reply.Err == OK {
				return true
			}
			if reply.Err == ErrNotReady {
				return false
			}
		}
	}
	return true
}

type ReconfigureArgs struct {
	Config    shardmaster.Config
	Data      [shardmaster.NShards]map[string]string
	LatestSeq map[int64]int64
}

//
// collect shards from other replica groups required for reconfiguration and build
// reconfigure log entry to append to raft log.
//
func (kv *ShardKV) getReconfigureEntry(nextConfig shardmaster.Config) (Op, bool) {
	entry := Op{}
	entry.Command = "reconfigure"
	entry.Config = nextConfig
	for i := 0; i < shardmaster.NShards; i++ {
		entry.Data[i] = make(map[string]string)
	}
	entry.LatestSeq = make(map[int64]int64)
	ok := true

	var ackMu sync.Mutex
	var wg sync.WaitGroup

	transferShards := kv.getShardsToTransfer(nextConfig)
	for gid, shardIds := range transferShards {
		wg.Add(1)

		go func(gid int, args TransferShardArgs, reply TransferShardReply) {
			defer wg.Done()

			if kv.sendTransferShard(gid, &args, &reply) {
				ackMu.Lock()
				// copy only shards requested from that replica group to reconfigure args
				for _, shardId := range args.ShardIds {
					shardData := reply.Data[shardId]
					for k, v := range shardData {
						entry.Data[shardId][k] = v
					}
				}
				// merge latestSequences map from that replica group to reconfigure args
				for clientId := range reply.LatestSeq {
					if _, ok := entry.LatestSeq[clientId]; !ok || entry.LatestSeq[clientId] < reply.LatestSeq[clientId] {
						entry.LatestSeq[clientId] = reply.LatestSeq[clientId]
					}
				}
				ackMu.Unlock()
			} else {
				ok = false
			}
		}(gid, TransferShardArgs{Num: nextConfig.Num, ShardIds: shardIds}, TransferShardReply{})
	}
	wg.Wait()
	return entry, ok
}

//
// build a map from gid to shard ids to request from replica group specified by gid.
//
func (kv *ShardKV) getShardsToTransfer(nextConfig shardmaster.Config) map[int][]int {
	transferShards := make(map[int][]int)
	for i := 0; i < shardmaster.NShards; i++ {
		if kv.config.Shards[i] != kv.gid && nextConfig.Shards[i] == kv.gid {
			gid := kv.config.Shards[i]
			if gid != 0 {
				if _, ok := transferShards[gid]; !ok {
					transferShards[gid] = make([]int, 0)
				}
				transferShards[gid] = append(transferShards[gid], i)
			}
		}
	}
	return transferShards
}

//
// if this server is leader of the replica group , it should query latest
// configuration periodically and try to update configuration if rquired.
//
func (kv *ShardKV) Reconfigure() {
	for {
		if _, isLeader := kv.rf.GetState(); isLeader {
			latestConfig := kv.mck.Query(-1)
			for i := kv.config.Num + 1; i <= latestConfig.Num; i++ {
				// apply configuration changes in order
				nextConfig := kv.mck.Query(i)
				entry, ok := kv.getReconfigureEntry(nextConfig)
				if !ok {
					break
				}
				result := kv.appendEntryToLog(entry)
				if !result.OK {
					break
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

type DeleteShardArgs struct {
	Num     int
	ShardId int
}

type DeleteShardReply struct {
	WrongLeader bool
	Err         Err
}

//
// if this server is ready, create an entry to delete the shard in args and try
// to append it to raft servers' log.
//
func (kv *ShardKV) DeleteShard(args *DeleteShardArgs, reply *DeleteShardReply) {
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.WrongLeader = true
		return
	}

	if args.Num > kv.config.Num {
		reply.WrongLeader = false
		reply.Err = ErrNotReady
		return
	}

	entry := Op{}
	entry.Command = "delete"
	entry.ConfigNum = args.Num
	entry.ShardId = args.ShardId
	kv.appendEntryToLog(entry)

	reply.WrongLeader = false
	reply.Err = OK
}

//
// try to ask replica group specified by gid in last config to delete the shard
// no longer required.
//
func (kv *ShardKV) sendDeleteShard(gid int, lastConfig *shardmaster.Config, args *DeleteShardArgs, reply *DeleteShardReply) bool {
	for _, server := range lastConfig.Groups[gid] {
		srv := kv.makeEnd(server)
		ok := srv.Call("ShardKV.DeleteShard", args, reply)
		if ok {
			if reply.Err == OK {
				return true
			}
			if reply.Err == ErrNotReady {
				return false
			}
		}
	}
	return true
}

func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, masters []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	gob.Register(Op{})
	gob.Register(Result{})
	gob.Register(shardmaster.Config{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.makeEnd = make_end
	kv.gid = gid
	kv.masters = masters

	kv.applyCh = make(chan raft.ApplyMsg, 100)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	for i := 0; i < shardmaster.NShards; i++ {
		kv.data[i] = make(map[string]string)
	}
	kv.latestSequences = make(map[int64]int64)
	kv.notifyCh = make(map[int]chan Result)
	kv.mck = shardmaster.MakeClerk(masters)

	go kv.Run()
	go kv.Reconfigure()

	return kv
}

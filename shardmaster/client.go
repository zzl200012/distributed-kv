package shardmaster

import (
	"crypto/rand"
	"distributed-kv/labrpc"
	"math/big"
	"sync"
	"time"
)

type Clerk struct {
	servers     []*labrpc.ClientEnd
	mu          sync.Mutex
	clientId    int64
	SequenceNum int64
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
	ck.clientId = nrand()
	ck.SequenceNum = 0
	return ck
}

func (ck *Clerk) Query(num int) Config {
	args := &QueryArgs{}
	args.Num = num
	args.ClientId = ck.clientId
	ck.mu.Lock()
	args.SequenceNum = ck.SequenceNum
	ck.SequenceNum++
	ck.mu.Unlock()

	for {
		for _, srv := range ck.servers {
			var reply QueryReply
			ok := srv.Call("ShardMaster.Query", args, &reply)
			if ok && reply.WrongLeader == false {
				return reply.Config
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	args := &JoinArgs{}
	args.Servers = servers
	args.ClientId = ck.clientId
	ck.mu.Lock()
	args.SequenceNum = ck.SequenceNum
	ck.SequenceNum++
	ck.mu.Unlock()

	for {
		for _, srv := range ck.servers {
			var reply JoinReply
			ok := srv.Call("ShardMaster.Join", args, &reply)
			if ok && reply.WrongLeader == false {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Leave(gids []int) {
	args := &LeaveArgs{}
	args.GIDs = gids
	args.ClientId = ck.clientId
	ck.mu.Lock()
	args.SequenceNum = ck.SequenceNum
	ck.SequenceNum++
	ck.mu.Unlock()

	for {
		for _, srv := range ck.servers {
			var reply LeaveReply
			ok := srv.Call("ShardMaster.Leave", args, &reply)
			if ok && reply.WrongLeader == false {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	args := &MoveArgs{}
	args.Shard = shard
	args.GID = gid
	args.ClientId = ck.clientId
	ck.mu.Lock()
	args.SequenceNum = ck.SequenceNum
	ck.SequenceNum++
	ck.mu.Unlock()

	for {
		for _, srv := range ck.servers {
			var reply MoveReply
			ok := srv.Call("ShardMaster.Move", args, &reply)
			if ok && reply.WrongLeader == false {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

package shardkv

const (
	OK            = "OK"
	ErrNoKey      = "ErrNoKey"
	ErrWrongGroup = "ErrWrongGroup"
	ErrNotReady   = "ErrNotReady"
)

type Err string

type PutAppendArgs struct {
	Key         string
	Value       string
	Op          string
	ClientId    int64
	SequenceNum int64
}

type PutAppendReply struct {
	WrongLeader bool
	Err         Err
}

type GetArgs struct {
	Key string
	ClientId  int64
	SequenceNum int64
}

type GetReply struct {
	WrongLeader bool
	Err         Err
	Value       string
}

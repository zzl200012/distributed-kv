# distributed-kv
* /labrpc: 基于 net/rpc 封装的 rpc 库，可以用于模拟各种网络故障
* /raft: 从零实现的一个 raft 库，主要包括以下 feature：
  * Leader Election
  * Log Replication
  * Log Compaction
  * 未实现 Membership Change
* /shardmaster: 起到注册中心的作用，存储一些服务器和分片相关的 metadata，内部实际上是以一个 raft 集群的形式对外提供服务，保证可用性
* /shardkv: 提供 KV 服务的模块，本质上是实现了一个 multi-raft，把键值对通过 hash 的方式分成若干个 shard，然后每个 shard 属于一个 raft group，每个 raft group 又以 raft 集群的形式复制到若干个服务器上，从而实现了高可用
* // TODO：实现细节
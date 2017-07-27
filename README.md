# Redis-cluster-watchdog

`redis-cluster-watchdog` can pretend as a redis cluster node which accept `RCmb(Redis Cluster message bus)` message and play with redis cluster.

# Run an example

## Create a redis cluster

```java  

$wget https://github.com/antirez/redis/archive/4.0.0.tar.gz
$tar -xvzf 4.0.0.tar.gz
$cd redis-4.0.0
$make MALLOC=libc
$cp src/redis-trib.rb /usr/local/bin/
$gem install redis
$cd utils/create-cluster
$./create-cluster start
$./create-cluster create

```

## Run redis-cluster-watchdog

```java  

    public static void main(String[] args) {
        ClusterWatchdog watchdog = new RedisClusterWatchdog(ClusterConfiguration.defaultSetting().setClusterAnnouncePort(10001));
        watchdog.start();
    }

```

## Add redis-cluster-watchdog to redis cluster as a normal node

```java  

$cd /path/to/redis-4.0.0/src
$./redis-cli -p 10001
127.0.0.1:10001>cluster meet 127.0.0.1 30001

```

## Supported commands

`CLUSTER MEET ip port <cport>`  
`CLUSTER NODES`  
`CLUSTER MYID`  
`CLUSTER SLOTS`  
`CLUSTER BUMPEPOCH`  
`CLUSTER INFO`  
`CLUSTER SAVECONFIG`  
`CLUSTER KEYSLOT`  
`CLUSTER FORGET nodename`  
`CLUSTER REPLICATE nodename`  
`CLUSTER SLAVES nodename`  
`CLUSTER COUNT-FAILURE-REPORTS nodename`  
`CLUSTER SET-CONFIG-EPOCH epoch`  
`CLUSTER RESET <HARD | SOFT>`  
  
Following command **MUST** open `ClusterConfiguration.asMaster`  
  
`CLUSTER ADDSLOTS slot <slot>`  
`CLUSTER DELSLOTS slot <slot>`  
`CLUSTER SETSLOT slot MIGRATING nodename`  
`CLUSTER SETSLOT slot IMPORTING nodename`  
`CLUSTER SETSLOT slot STABLE`  
`CLUSTER SETSLOT slot NODE nodename`  
`CLUSTER GETKEYSINSLOT slot count` will always return a zero length array  
`CLUSTER COUNTKEYSINSLOT slot` will always return zero  

## Supported redis-trib.rb command

`redis-trib.rb create <--replicas N> host1:port1 ... hostN:portN`  
`redis-trib.rb check host:port`  
`redis-trib.rb info host:port`  
`redis-trib.rb rebalance <--weight N> --auto-weights --threshold N --use-empty-masters --simulate --timeout milliseconds --pipeline N`  
`redis-trib.rb reshard --from arg --to nodename --slots N --yes --timeout milliseconds --pipeline N host:port`  
`redis-trib.rb add-node <--slave --master-id masterid> new_host:new_port existing_host:existing_port`  
`redis-trib.rb del-node host:port node_id`  
`redis-trib.rb call host:port command arg arg .. arg`  
`redis-trib.rb set-timeout host:port milliseconds`  
`redis-trib.rb import --from host:port <--copy> <--replace> host:port`  
`redis-trib.rb help`  

## Listeners  

`ReplicationListener`  
`ClusterConfigListener`  
`ClusterStateListener`  
`RestoreCommandListener`  
`ClusterNodeFailedListener`  

# Have fun!!
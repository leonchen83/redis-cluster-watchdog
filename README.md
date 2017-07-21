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

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        ClusterManagers managers = new ClusterManagers(new ClusterConfiguration().setClusterAnnouncePort(10001));
        ThinServer client = new ThinServer(managers);
        ThinGossip gossip = new ThinGossip(managers);
        client.start();
        gossip.start();
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

## Listeners  

`ReplicationListener`  

# Have fun!!
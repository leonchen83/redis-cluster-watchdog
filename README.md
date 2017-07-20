# redis-replicator-cluster

`redis-replicator-cluster` can pretend as a redis cluster node which accept `RCmb` message.

# play with it

## create a redis cluster

```java  

$wget https://github.com/antirez/redis/archive/4.0.0.tar.gz
$tar -xvzf 4.0.0.tar.gz
$cd redis-4.0.0
$make MALLOC=libc
$cd utils/create-cluster
$./create-cluster start
$./create-cluster create

```

## run redis-replicator-cluster

```java  

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        ClusterManagers managers = new ClusterManagers(new ClusterConfiguration().setClusterAnnouncePort(10001));
        ThinServer client = new ThinServer(managers);
        ThinGossip gossip = new ThinGossip(managers);
        client.start();
        gossip.start();
    }

```

## add redis-replicator-cluster to redis cluster

```java  

$cd /path/to/redis-4.0.0/src
$./redis-cli -p 10001
127.0.0.1:10001>cluster meet 127.0.0.1 30001

```

# Have fun!!
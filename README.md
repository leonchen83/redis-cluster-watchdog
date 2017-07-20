# redis-cluster-watchdog

`redis-cluster-watchdog` can pretend as a redis cluster node which accept `RCmb(Redis Cluster message bus)` message and play with redis cluster.

# run an example

## create a redis cluster

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

## run redis-cluster-watchdog

```java  

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        ClusterManagers managers = new ClusterManagers(new ClusterConfiguration().setClusterAnnouncePort(10001));
        ThinServer client = new ThinServer(managers);
        ThinGossip gossip = new ThinGossip(managers);
        client.start();
        gossip.start();
    }

```

## add redis-cluster-watchdog to redis cluster as a normal node

```java  

$cd /path/to/redis-4.0.0/src
$./redis-cli -p 10001
127.0.0.1:10001>cluster meet 127.0.0.1 30001

```

# Have fun!!
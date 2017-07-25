package com.moilioncircle.redis.cluster.watchdog;

/**
 * @author Leon Chen
 * @since 1.0.0
 */
public class ReplicationListenerTest {
    public static void main(String[] args) {
        ClusterWatchdog watchdog = new RedisClusterWatchdog(ClusterConfiguration.defaultSetting());
        watchdog.setReplicationListener(new TestReplicationListener());
        watchdog.start();
    }

}
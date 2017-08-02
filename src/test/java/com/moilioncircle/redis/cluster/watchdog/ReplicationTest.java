package com.moilioncircle.redis.cluster.watchdog;

import com.moilioncircle.redis.cluster.watchdog.storage.RedisStorageEngine;

/**
 * @author Leon Chen
 * @since 1.0.0
 */
public class ReplicationTest {
    public static void main(String[] args) {
        ClusterConfiguration configuration = ClusterConfiguration.defaultSetting();
        configuration.setFailover(true).setVersion(Version.PROTOCOL_V0).setClusterAnnouncePort(20000);

        ClusterWatchdog watchdog = new RedisClusterWatchdog(configuration);
        watchdog.setStorageEngine(new RedisStorageEngine());
        watchdog.setClusterConfigListener(System.out::println);
        watchdog.setReplicationListener(new SimpleReplicationListener());
        watchdog.start();
    }
}
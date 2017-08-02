package com.moilioncircle.redis.cluster.watchdog;

/**
 * @author Leon Chen
 * @since 1.0.0
 */
public class ReplicationTest {
    public static void main(String[] args) {
        ClusterConfiguration configuration = ClusterConfiguration.defaultSetting();
        configuration.setFailover(true).setVersion(Version.PROTOCOL_V0);

        ClusterWatchdog watchdog = new RedisClusterWatchdog(configuration);
        watchdog.setClusterConfigListener(System.out::println);
        watchdog.setReplicationListener(new SimpleReplicationListener());
        watchdog.start();
    }
}
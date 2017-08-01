package com.moilioncircle.redis.cluster.watchdog;

/**
 * @author Leon Chen
 * @since 1.0.0
 */
public class ReplicationListenerTest {
    public static void main(String[] args) {
        ClusterConfiguration configuration = ClusterConfiguration.defaultSetting();
        configuration.setMaster(true).setVersion(Version.PROTOCOL_V0);

        ClusterWatchdog watchdog = new RedisClusterWatchdog(configuration);
        watchdog.setClusterConfigListener(System.out::println);
        watchdog.setReplicationListener(new SimpleReplicationListener());
        watchdog.setRestoreCommandListener((kv, replace) -> {
            System.out.println(kv);
            System.out.println(replace);
        });
        watchdog.start();
    }

}
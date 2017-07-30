package com.moilioncircle.redis.cluster.watchdog;

import com.moilioncircle.redis.replicator.rdb.datatype.KeyValuePair;

/**
 * @author Leon Chen
 * @since 1.0.0
 */
public class ReplicationListenerTest {
    public static void main(String[] args) {
        ClusterWatchdog watchdog = new RedisClusterWatchdog(ClusterConfiguration.defaultSetting().setMaster(true).setVersion(Version.PROTOCOL_V0));
        watchdog.setReplicationListener(new TestReplicationListener());
        watchdog.setRestoreCommandListener(new RestoreCommandListener() {
            @Override
            public void onRestoreCommand(KeyValuePair<?> kv, boolean replace) {
                System.out.println(kv);
                System.out.println(replace);
            }
        });
        watchdog.start();
    }

}
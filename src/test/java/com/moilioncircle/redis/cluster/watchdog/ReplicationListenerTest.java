package com.moilioncircle.redis.cluster.watchdog;

import com.moilioncircle.redis.cluster.watchdog.manager.ClusterManagers;

/**
 * @author Leon Chen
 * @since 1.0.0
 */
public class ReplicationListenerTest {
    public static void main(String[] args) {
        ClusterManagers managers = new ClusterManagers(ClusterConfiguration.defaultSetting());
        managers.setReplicationListener(new TestReplicationListener());
        ThinServer client = new ThinServer(managers);
        ThinGossip gossip = new ThinGossip(managers);
        client.start();
        gossip.start();
    }

}
package com.moilioncircle.redis.cluster.watchdog.listener;

import com.moilioncircle.redis.cluster.watchdog.ClusterConfiguration;
import com.moilioncircle.redis.cluster.watchdog.ThinGossip;
import com.moilioncircle.redis.cluster.watchdog.ThinServer;
import com.moilioncircle.redis.cluster.watchdog.manager.ClusterManagers;

/**
 * @author Leon Chen
 * @since 2.1.0
 */
public class ReplicationListenerTest {
    public static void main(String[] args) {
        ClusterManagers managers = new ClusterManagers(new ClusterConfiguration());
        managers.setReplicationListener(new TestReplicationListener());
        ThinServer client = new ThinServer(managers);
        ThinGossip gossip = new ThinGossip(managers);
        client.start();
        gossip.start();
    }

}
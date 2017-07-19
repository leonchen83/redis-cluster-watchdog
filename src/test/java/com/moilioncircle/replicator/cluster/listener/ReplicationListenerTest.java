package com.moilioncircle.replicator.cluster.listener;

import com.moilioncircle.replicator.cluster.ClusterConfiguration;
import com.moilioncircle.replicator.cluster.ThinGossip;
import com.moilioncircle.replicator.cluster.ThinServer;
import com.moilioncircle.replicator.cluster.manager.ClusterManagers;

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
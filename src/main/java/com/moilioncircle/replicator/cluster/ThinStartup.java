package com.moilioncircle.replicator.cluster;

import com.moilioncircle.replicator.cluster.manager.ClusterManagers;

import java.util.concurrent.ExecutionException;

/**
 * Created by Baoyi Chen on 2017/7/14.
 */
public class ThinStartup {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        ClusterManagers managers = new ClusterManagers(new ClusterConfiguration());
        ThinServer client = new ThinServer(managers);
        ThinGossip gossip = new ThinGossip(managers);
        client.start();
        gossip.start();
    }
}

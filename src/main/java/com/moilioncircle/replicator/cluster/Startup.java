package com.moilioncircle.replicator.cluster;

import com.moilioncircle.replicator.cluster.gossip.ThinGossip;

import java.util.concurrent.ExecutionException;

/**
 * Created by Baoyi Chen on 2017/7/14.
 */
public class Startup {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        ThinGossip gossip = new ThinGossip(new ClusterConfiguration());
        gossip.start();
    }
}

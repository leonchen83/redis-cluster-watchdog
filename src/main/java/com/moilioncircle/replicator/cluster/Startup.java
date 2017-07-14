package com.moilioncircle.replicator.cluster;

import com.moilioncircle.replicator.cluster.gossip.ThinGossip;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Created by Baoyi Chen on 2017/7/14.
 */
public class Startup {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
        ThinGossip gossip = new ThinGossip(new ClusterConfiguration(), executor);

        gossip.start();
    }
}

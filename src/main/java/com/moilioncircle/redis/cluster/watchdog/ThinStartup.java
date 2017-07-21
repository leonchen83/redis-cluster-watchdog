package com.moilioncircle.redis.cluster.watchdog;

import com.moilioncircle.redis.cluster.watchdog.manager.ClusterManagers;

import java.util.concurrent.ExecutionException;

/**
 * @author Leon Chen
 * @since 1.0.0
 */
public class ThinStartup {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        ClusterManagers managers = new ClusterManagers(ClusterConfiguration.defaultSetting());
        ThinServer client = new ThinServer(managers);
        ThinGossip gossip = new ThinGossip(managers);
        client.start();
        gossip.start();
    }
}

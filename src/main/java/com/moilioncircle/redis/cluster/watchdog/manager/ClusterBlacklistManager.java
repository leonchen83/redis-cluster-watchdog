package com.moilioncircle.redis.cluster.watchdog.manager;

import com.moilioncircle.redis.cluster.watchdog.ClusterConstants;
import com.moilioncircle.redis.cluster.watchdog.state.ClusterNode;
import com.moilioncircle.redis.cluster.watchdog.state.ServerState;
import com.moilioncircle.redis.cluster.watchdog.util.Tuples;

/**
 * Created by Baoyi Chen on 2017/7/13.
 */
public class ClusterBlacklistManager {

    private ServerState server;
    private ClusterManagers managers;

    public ClusterBlacklistManager(ClusterManagers managers) {
        this.managers = managers;
        this.server = managers.server;
    }

    public void clusterBlacklistCleanup() {
        server.cluster.nodesBlackList.values().removeIf(e -> e.getV1() < System.currentTimeMillis());
    }

    public void clusterBlacklistAddNode(ClusterNode node) {
        clusterBlacklistCleanup();
        server.cluster.nodesBlackList.put(node.name, Tuples.of(System.currentTimeMillis() + ClusterConstants.CLUSTER_BLACKLIST_TTL, node));
    }

    public boolean clusterBlacklistExists(String nodename) {
        clusterBlacklistCleanup();
        return server.cluster.nodesBlackList.containsKey(nodename);
    }
}

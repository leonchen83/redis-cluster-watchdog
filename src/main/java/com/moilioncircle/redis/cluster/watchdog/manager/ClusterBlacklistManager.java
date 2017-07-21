package com.moilioncircle.redis.cluster.watchdog.manager;

import com.moilioncircle.redis.cluster.watchdog.state.ClusterNode;
import com.moilioncircle.redis.cluster.watchdog.state.ServerState;

import static com.moilioncircle.redis.cluster.watchdog.ClusterConstants.CLUSTER_BLACKLIST_TTL;
import static com.moilioncircle.redis.cluster.watchdog.util.Tuples.of;

/**
 * @author Leon Chen
 * @since 1.0.0
 */
public class ClusterBlacklistManager {

    private ServerState server;

    public ClusterBlacklistManager(ClusterManagers managers) {
        this.server = managers.server;
    }

    public void clusterBlacklistCleanup() {
        server.cluster.blacklist.values().removeIf(e -> e.getV1() < System.currentTimeMillis());
    }

    public void clusterBlacklistAddNode(ClusterNode node) {
        clusterBlacklistCleanup();
        server.cluster.blacklist.put(node.name, of(System.currentTimeMillis() + CLUSTER_BLACKLIST_TTL, node));
    }

    public boolean clusterBlacklistExists(String name) {
        clusterBlacklistCleanup();
        return server.cluster.blacklist.containsKey(name);
    }
}

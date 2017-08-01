package com.moilioncircle.redis.cluster.watchdog.manager;

import com.moilioncircle.redis.cluster.watchdog.state.ClusterNode;
import com.moilioncircle.redis.cluster.watchdog.state.ServerState;
import com.moilioncircle.redis.cluster.watchdog.util.type.Tuple2;

import java.util.Map;

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

    public boolean clusterBlacklistExists(String name) {
        long now = System.currentTimeMillis();
        Map<String, Tuple2<Long, ClusterNode>> map = server.cluster.blacklist;
        map.values().removeIf(e -> e.getV1() < now); return map.containsKey(name);
    }

    public void clusterBlacklistAddNode(ClusterNode node) {
        long now = System.currentTimeMillis();
        server.cluster.blacklist.values().removeIf(e -> e.getV1() < now);
        server.cluster.blacklist.put(node.name, of(now + CLUSTER_BLACKLIST_TTL, node));
    }
}

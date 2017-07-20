package com.moilioncircle.redis.cluster.watchdog.state;

import static com.moilioncircle.redis.cluster.watchdog.ClusterConstants.*;

/**
 * Created by Baoyi Chen on 2017/7/19.
 */
public class States {
    public static boolean nodeIsMaster(ClusterNode n) {
        return (n.flags & CLUSTER_NODE_MASTER) != 0;
    }

    public static boolean nodeIsSlave(ClusterNode n) {
        return (n.flags & CLUSTER_NODE_SLAVE) != 0;
    }

    public static boolean nodeInHandshake(ClusterNode n) {
        return (n.flags & CLUSTER_NODE_HANDSHAKE) != 0;
    }

    public static boolean nodeHasAddr(ClusterNode n) {
        return (n.flags & CLUSTER_NODE_NOADDR) == 0;
    }

    public static boolean nodeWithoutAddr(ClusterNode n) {
        return (n.flags & CLUSTER_NODE_NOADDR) != 0;
    }

    public static boolean nodePFailed(ClusterNode n) {
        return (n.flags & CLUSTER_NODE_PFAIL) != 0;
    }

    public static boolean nodeFailed(ClusterNode n) {
        return (n.flags & CLUSTER_NODE_FAIL) != 0;
    }
}

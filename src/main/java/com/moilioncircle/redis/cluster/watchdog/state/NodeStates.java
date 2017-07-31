package com.moilioncircle.redis.cluster.watchdog.state;

import static com.moilioncircle.redis.cluster.watchdog.ClusterConstants.*;

/**
 * @author Leon Chen
 * @since 1.0.0
 */
public class NodeStates {
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

    /**
     *
     */
    public static boolean nodeIsMaster(int flags) {
        return (flags & CLUSTER_NODE_MASTER) != 0;
    }

    public static boolean nodeIsSlave(int flags) {
        return (flags & CLUSTER_NODE_SLAVE) != 0;
    }

    public static boolean nodePFailed(int flags) {
        return (flags & CLUSTER_NODE_PFAIL) != 0;
    }

    public static boolean nodeFailed(int flags) {
        return (flags & CLUSTER_NODE_FAIL) != 0;
    }

    public static boolean nodeIsMyself(int flags) {
        return (flags & CLUSTER_NODE_MYSELF) != 0;
    }

    public static boolean nodeInHandshake(int flags) {
        return (flags & CLUSTER_NODE_HANDSHAKE) != 0;
    }

    public static boolean nodeHasAddr(int flags) {
        return (flags & CLUSTER_NODE_NOADDR) == 0;
    }

    public static boolean nodeWithoutAddr(int flags) {
        return (flags & CLUSTER_NODE_NOADDR) != 0;
    }

    public static boolean nodeInMeet(int flags) {
        return (flags & CLUSTER_NODE_MEET) != 0;
    }

    public static boolean nodeInMigrate(int flags) {
        return (flags & CLUSTER_NODE_MIGRATE_TO) != 0;
    }
}

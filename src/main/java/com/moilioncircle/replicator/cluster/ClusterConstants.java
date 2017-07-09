package com.moilioncircle.replicator.cluster;

/**
 * Created by Baoyi Chen on 2017/7/6.
 */
public class ClusterConstants {
    public static final int CLUSTER_SLOTS = 16384;
    public static final int CLUSTER_OK = 0;          /* Everything looks ok */
    public static final int CLUSTER_FAIL = 1;        /* The cluster can't work */
    public static final int CLUSTER_NAMELEN = 40;    /* sha1 hex length */
    public static final int CLUSTER_PORT_INCR = 10000; /* Cluster port = baseport + PORT_INCR */

    public static final int CLUSTER_PROTO_VER = 1;

    public static final int CLUSTER_DEFAULT_NODE_TIMEOUT = 15000;
    public static final int CLUSTER_DEFAULT_SLAVE_VALIDITY = 10; /* Slave max data age factor. */
    public static final int CLUSTER_DEFAULT_REQUIRE_FULL_COVERAGE = 1;
    public static final int CLUSTER_FAIL_REPORT_VALIDITY_MULT = 2; /* Fail report validity. */
    public static final int CLUSTER_FAIL_UNDO_TIME_MULT = 2; /* Undo fail if master is back. */
    public static final int CLUSTER_FAIL_UNDO_TIME_ADD = 10; /* Some additional time. */
    public static final int CLUSTER_FAILOVER_DELAY = 5; /* Seconds */
    public static final int CLUSTER_DEFAULT_MIGRATION_BARRIER = 1;
    public static final int CLUSTER_MF_TIMEOUT = 5000; /* Milliseconds to do a manual failover. */
    public static final int CLUSTER_MF_PAUSE_MULT = 2; /* Master pause manual failover mult. */
    public static final int CLUSTER_SLAVE_MIGRATION_DELAY = 5000; /* Delay for slave migration. */

    public static final int CLUSTER_REDIR_NONE = 0;          /* Node can serve the request. */
    public static final int CLUSTER_REDIR_CROSS_SLOT = 1;    /* -CROSSSLOT request. */
    public static final int CLUSTER_REDIR_UNSTABLE = 2;      /* -TRYAGAIN redirection required */
    public static final int CLUSTER_REDIR_ASK = 3;           /* -ASK redirection required. */
    public static final int CLUSTER_REDIR_MOVED = 4;         /* -MOVED redirection required. */
    public static final int CLUSTER_REDIR_DOWN_STATE = 5;    /* -CLUSTERDOWN, global state. */
    public static final int CLUSTER_REDIR_DOWN_UNBOUND = 6;  /* -CLUSTERDOWN, unbound slot. */

    public static final int CLUSTER_NODE_MASTER = 1;     /* The node is a master */
    public static final int CLUSTER_NODE_SLAVE = 2;      /* The node is a slave */
    public static final int CLUSTER_NODE_PFAIL = 4;      /* Failure? Need acknowledge */
    public static final int CLUSTER_NODE_FAIL = 8;       /* The node is believed to be malfunctioning */
    public static final int CLUSTER_NODE_MYSELF = 16;    /* This node is myself */
    public static final int CLUSTER_NODE_HANDSHAKE = 32; /* We have still to exchange the first ping */
    public static final int CLUSTER_NODE_NOADDR = 64;  /* We don't know the address of this node */
    public static final int CLUSTER_NODE_MEET = 128;     /* Send a MEET message to this node */
    public static final int CLUSTER_NODE_MIGRATE_TO = 256; /* Master elegible for replica migration. */
    public static final String CLUSTER_NODE_NULL_NAME = "0000000000000000000000000000000000000000";

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

    public static boolean nodeTimedOut(ClusterNode n) {
        return (n.flags & CLUSTER_NODE_PFAIL) != 0;
    }

    public static boolean nodeFailed(ClusterNode n) {
        return (n.flags & CLUSTER_NODE_FAIL) != 0;
    }

    public static final int CLUSTER_CANT_FAILOVER_NONE = 0;
    public static final int CLUSTER_CANT_FAILOVER_DATA_AGE = 1;
    public static final int CLUSTER_CANT_FAILOVER_WAITING_DELAY = 2;
    public static final int CLUSTER_CANT_FAILOVER_EXPIRED = 3;
    public static final int CLUSTER_CANT_FAILOVER_WAITING_VOTES = 4;
    public static final int CLUSTER_CANT_FAILOVER_RELOG_PERIOD = 300; /* seconds. */

    public static final int CLUSTER_TODO_HANDLE_FAILOVER = (1 << 0);
    public static final int CLUSTER_TODO_UPDATE_STATE = (1 << 1);
    public static final int CLUSTER_TODO_SAVE_CONFIG = (1 << 2);
    public static final int CLUSTER_TODO_FSYNC_CONFIG = (1 << 3);

    public static final int CLUSTERMSG_TYPE_PING = 0;          /* Ping */
    public static final int CLUSTERMSG_TYPE_PONG = 1;          /* Pong (reply to Ping) */
    public static final int CLUSTERMSG_TYPE_MEET = 2;          /* Meet "let's join" message */
    public static final int CLUSTERMSG_TYPE_FAIL = 3;          /* Mark node xxx as failing */
    public static final int CLUSTERMSG_TYPE_PUBLISH = 4;       /* Pub/Sub Publish propagation */
    public static final int CLUSTERMSG_TYPE_FAILOVER_AUTH_REQUEST = 5; /* May I failover? */
    public static final int CLUSTERMSG_TYPE_FAILOVER_AUTH_ACK = 6;     /* Yes, you have my vote */
    public static final int CLUSTERMSG_TYPE_UPDATE = 7;        /* Another node slots configuration */
    public static final int CLUSTERMSG_TYPE_MFSTART = 8;       /* Pause clients for manual failover */
    public static final int CLUSTERMSG_TYPE_COUNT = 9;

    public static final int CLUSTERMSG_MIN_LEN = 100; //TODO

    public static final int CLUSTERMSG_FLAG0_PAUSED = (1 << 0); /* Master paused for manual failover. */
    public static final int CLUSTERMSG_FLAG0_FORCEACK = (1 << 1); /* Give ACK to AUTH_REQUEST even if is up. *//* Total number of message types. */

    public static final int CLUSTER_BLACKLIST_TTL = 60;
}

package com.moilioncircle.redis.cluster.watchdog;

import java.nio.charset.Charset;

/**
 * @author Leon Chen
 * @since 1.0.0
 */
public class ClusterConstants {

    public static final Charset CHARSET = Charset.forName("UTF-8");

    public static final char[] HEX_CHARS = new char[]{'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'};

    public static final int CLUSTER_SLOTS = 16384;

    public static final int CLUSTER_SLOTS_BYTES = 16384 / 8;

    public static final byte CLUSTER_OK = 0;

    public static final byte CLUSTER_FAIL = 1;

    public static final int CLUSTER_NAME_LEN = 40;

    public static final int CLUSTER_PORT_INCR = 10000;

    public static final int CLUSTER_PROTOCOL_VERSION = 1;

    public static final int CLUSTER_FAIL_REPORT_VALIDITY_MULTI = 2;

    public static final int CLUSTER_FAIL_UNDO_TIME_MULTI = 2;

    public static final int CLUSTER_SLAVE_MIGRATION_DELAY = 5000;

    public static final int CLUSTER_NODE_MASTER = 1;

    public static final int CLUSTER_NODE_SLAVE = 2;

    public static final int CLUSTER_NODE_PFAIL = 4;

    public static final int CLUSTER_NODE_FAIL = 8;

    public static final int CLUSTER_NODE_MYSELF = 16;

    public static final int CLUSTER_NODE_HANDSHAKE = 32;

    public static final int CLUSTER_NODE_NOADDR = 64;

    public static final int CLUSTER_NODE_MEET = 128;

    public static final int CLUSTER_NODE_MIGRATE_TO = 256;

    public static final byte[] CLUSTER_NODE_NULL_NAME = new byte[40];

    public static final byte[] CLUSTER_NODE_NULL_IP = new byte[46];

    public static final int CLUSTERMSG_TYPE_PING = 0;

    public static final int CLUSTERMSG_TYPE_PONG = 1;

    public static final int CLUSTERMSG_TYPE_MEET = 2;

    public static final int CLUSTERMSG_TYPE_FAIL = 3;

    public static final int CLUSTERMSG_TYPE_PUBLISH = 4;

    public static final int CLUSTERMSG_TYPE_FAILOVER_AUTH_REQUEST = 5;

    public static final int CLUSTERMSG_TYPE_FAILOVER_AUTH_ACK = 6;

    public static final int CLUSTERMSG_TYPE_UPDATE = 7;

    public static final int CLUSTERMSG_TYPE_MFSTART = 8;

    public static final int CLUSTERMSG_TYPE_COUNT = 9;

    public static final int CLUSTER_BLACKLIST_TTL = 60000;

    public static final int CLUSTER_MAX_REJOIN_DELAY = 5000;

    public static final int CLUSTER_MIN_REJOIN_DELAY = 500;

    public static final int CLUSTER_WRITABLE_DELAY = 2000;

    public static final int CLUSTERMSG_FLAG0_FORCEACK = (1 << 1);

    public static final int CLUSTER_BROADCAST_ALL = 0;

    public static final int CLUSTER_BROADCAST_LOCAL_SLAVES = 1;
}

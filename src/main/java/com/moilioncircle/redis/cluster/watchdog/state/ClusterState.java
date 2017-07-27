package com.moilioncircle.redis.cluster.watchdog.state;

import com.moilioncircle.redis.cluster.watchdog.util.type.Tuple2;

import java.util.LinkedHashMap;
import java.util.Map;

import static com.moilioncircle.redis.cluster.watchdog.ClusterConstants.CLUSTERMSG_TYPE_COUNT;
import static com.moilioncircle.redis.cluster.watchdog.ClusterConstants.CLUSTER_SLOTS;
import static com.moilioncircle.redis.cluster.watchdog.ClusterState.CLUSTER_FAIL;

/**
 * @author Leon Chen
 * @since 1.0.0
 */
public class ClusterState {
    public int size = 1;
    public long pFailNodes = 0;
    public long currentEpoch = 0;
    public long lastVoteEpoch = 0;
    public int failoverAuthRank = 0;
    public ClusterNode myself = null;
    public long failoverAuthTime = 0;
    public int failoverAuthCount = 0;
    public long failoverAuthEpoch = 0;
    public boolean failoverAuthSent = false;
    public ClusterNode[] slots = new ClusterNode[CLUSTER_SLOTS];
    public Map<String, ClusterNode> nodes = new LinkedHashMap<>();
    public long[] messagesSent = new long[CLUSTERMSG_TYPE_COUNT];
    public long[] messagesReceived = new long[CLUSTERMSG_TYPE_COUNT];
    public ClusterNode[] migrating = new ClusterNode[CLUSTER_SLOTS];
    public ClusterNode[] importing = new ClusterNode[CLUSTER_SLOTS];
    public Map<String, Tuple2<Long, ClusterNode>> blacklist = new LinkedHashMap<>();
    public com.moilioncircle.redis.cluster.watchdog.ClusterState state = CLUSTER_FAIL;
}

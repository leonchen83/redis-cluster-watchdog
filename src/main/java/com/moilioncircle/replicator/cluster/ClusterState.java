package com.moilioncircle.replicator.cluster;

import java.util.Map;

import static com.moilioncircle.replicator.cluster.ClusterConstants.CLUSTERMSG_TYPE_COUNT;
import static com.moilioncircle.replicator.cluster.ClusterConstants.CLUSTER_SLOTS;

/**
 * Created by Baoyi Chen on 2017/7/6.
 */
public class ClusterState {
    public ClusterNode myself;
    public long currentEpoch;
    public int state;
    public int size;
    public Map<String, ClusterNode> nodes;
    public Map<String, Map.Entry<Long, ClusterNode>> nodesBlackList;
    public ClusterNode[] slots = new ClusterNode[CLUSTER_SLOTS];
    public int todoBeforeSleep;
    public long[] statsBusMessagesSent = new long[CLUSTERMSG_TYPE_COUNT];
    public long[] statsBusMessagesReceived = new long[CLUSTERMSG_TYPE_COUNT];
    public long statsPfailNodes;
}

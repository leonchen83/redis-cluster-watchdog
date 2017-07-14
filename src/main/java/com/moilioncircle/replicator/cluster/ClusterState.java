package com.moilioncircle.replicator.cluster;

import java.util.Map;

import static com.moilioncircle.replicator.cluster.ClusterConstants.CLUSTERMSG_TYPE_COUNT;
import static com.moilioncircle.replicator.cluster.ClusterConstants.CLUSTER_SLOTS;

/**
 * Created by Baoyi Chen on 2017/7/6.
 */
public class ClusterState {
    public int size;
    public int state;
    public long currentEpoch;
    public ClusterNode myself;
    public int todoBeforeSleep;
    public long statsPfailNodes;
    public Map<String, ClusterNode> nodes;
    public ClusterNode[] slots = new ClusterNode[CLUSTER_SLOTS];
    public Map<String, Map.Entry<Long, ClusterNode>> nodesBlackList;
    public long[] statsBusMessagesSent = new long[CLUSTERMSG_TYPE_COUNT];
    public long[] statsBusMessagesReceived = new long[CLUSTERMSG_TYPE_COUNT];
}

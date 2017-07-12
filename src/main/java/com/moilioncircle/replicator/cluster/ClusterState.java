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
    public Map<String, ClusterNode> nodesBlackList;
    public ClusterNode[] migratingSlotsTo = new ClusterNode[CLUSTER_SLOTS];
    public ClusterNode[] importingSlotsFrom = new ClusterNode[CLUSTER_SLOTS];
    public ClusterNode[] slots = new ClusterNode[CLUSTER_SLOTS];
    //TODO public long[] slotsKeysCount = new long[CLUSTER_SLOTS];
    //TODO rax *slots_to_keys;
    public long failoverAuthTime;
    public int failoverAuthCount;
    public boolean failoverAuthSent;
    public int failoverAuthRank;
    public long failoverAuthEpoch;
    public int cantFailoverReason;
    public long mfEnd;
    public ClusterNode mfSlave;
    public long mfMasterOffset;
    public boolean mfCanStart;
    public long lastVoteEpoch;
    public int todoBeforeSleep;
    public long[] statsBusMessagesSent = new long[CLUSTERMSG_TYPE_COUNT];
    public long[] statsBusMessagesReceived = new long[CLUSTERMSG_TYPE_COUNT];
    public long statsPfailNodes;
}

package com.moilioncircle.replicator.cluster;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

import static com.moilioncircle.replicator.cluster.ClusterConstants.*;

/**
 * Created by Baoyi Chen on 2017/7/6.
 */
public class ClusterState {
    public int size = 1;
    public long currentEpoch = 0;
    public long statsPfailNodes = 0;
    public byte state = CLUSTER_FAIL;
    public ClusterNode myself = null;
    public Map<String, ClusterNode> nodes = new LinkedHashMap<>();
    public ClusterNode[] slots = new ClusterNode[CLUSTER_SLOTS];
    public long[] statsBusMessagesSent = new long[CLUSTERMSG_TYPE_COUNT];
    public long[] statsBusMessagesReceived = new long[CLUSTERMSG_TYPE_COUNT];
    public Map<String, Map.Entry<Long, ClusterNode>> nodesBlackList = new LinkedHashMap<>();

    @Override
    public String toString() {
        return "ClusterState{" +
                "size=" + size +
                ", state=" + state +
                ", currentEpoch=" + currentEpoch +
                ", myself=" + myself +
                ", statsPfailNodes=" + statsPfailNodes +
                ", nodes=" + nodes +
                ", slots=" + Arrays.toString(slots) +
                ", nodesBlackList=" + nodesBlackList +
                ", statsBusMessagesSent=" + Arrays.toString(statsBusMessagesSent) +
                ", statsBusMessagesReceived=" + Arrays.toString(statsBusMessagesReceived) +
                '}';
    }
}

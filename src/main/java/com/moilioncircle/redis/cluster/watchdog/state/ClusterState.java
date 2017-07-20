package com.moilioncircle.redis.cluster.watchdog.state;

import com.moilioncircle.redis.cluster.watchdog.ClusterConstants;
import com.moilioncircle.redis.cluster.watchdog.util.type.Tuple2;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Created by Baoyi Chen on 2017/7/6.
 */
public class ClusterState {
    public int size = 1;
    public long currentEpoch = 0;
    public long lastVoteEpoch = 0;
    public long statsPfailNodes = 0;
    public byte state = ClusterConstants.CLUSTER_FAIL;
    public ClusterNode myself = null;
    public ClusterNode[] slots = new ClusterNode[ClusterConstants.CLUSTER_SLOTS];
    public Map<String, ClusterNode> nodes = new LinkedHashMap<>();
    public long[] statsBusMessagesSent = new long[ClusterConstants.CLUSTERMSG_TYPE_COUNT];
    public long[] statsBusMessagesReceived = new long[ClusterConstants.CLUSTERMSG_TYPE_COUNT];
    public Map<String, Tuple2<Long, ClusterNode>> nodesBlackList = new LinkedHashMap<>();

    @Override
    public String toString() {
        return "ClusterState{" +
                "size=" + size +
                ", state=" + state +
                ", currentEpoch=" + currentEpoch +
                ", myself=" + myself +
                ", statsPfailNodes=" + statsPfailNodes +
                ", nodes=" + nodes +
                ", nodesBlackList=" + nodesBlackList +
                ", statsBusMessagesSent=" + Arrays.toString(statsBusMessagesSent) +
                ", statsBusMessagesReceived=" + Arrays.toString(statsBusMessagesReceived) +
                '}';
    }
}

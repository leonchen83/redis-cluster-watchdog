package com.moilioncircle.redis.cluster.watchdog.state;

import com.moilioncircle.redis.cluster.watchdog.util.type.Tuple2;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

import static com.moilioncircle.redis.cluster.watchdog.ClusterConstants.*;

/**
 * @author Leon Chen
 * @since 1.0.0
 */
public class ClusterState {
    public int size = 1;
    public long pFailNodes = 0;
    public long currentEpoch = 0;
    public long lastVoteEpoch = 0;
    public byte state = CLUSTER_FAIL;
    public ClusterNode myself = null;
    public ClusterNode[] slots = new ClusterNode[CLUSTER_SLOTS];
    public Map<String, ClusterNode> nodes = new LinkedHashMap<>();
    public long[] messagesSent = new long[CLUSTERMSG_TYPE_COUNT];
    public long[] messagesReceived = new long[CLUSTERMSG_TYPE_COUNT];
    public Map<String, Tuple2<Long, ClusterNode>> blacklist = new LinkedHashMap<>();

    @Override
    public String toString() {
        return "ClusterState{" +
                "size=" + size +
                ", state=" + state +
                ", currentEpoch=" + currentEpoch +
                ", myself=" + myself +
                ", pFailNodes=" + pFailNodes +
                ", nodes=" + nodes +
                ", blacklist=" + blacklist +
                ", messagesSent=" + Arrays.toString(messagesSent) +
                ", messagesReceived=" + Arrays.toString(messagesReceived) +
                '}';
    }
}

package com.moilioncircle.redis.cluster.watchdog;

import com.moilioncircle.redis.cluster.watchdog.state.ClusterNode;
import com.moilioncircle.redis.cluster.watchdog.state.ClusterState;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

import static com.moilioncircle.redis.cluster.watchdog.ClusterConstants.CLUSTER_SLOTS;

/**
 * @author Leon Chen
 * @since 1.0.0
 */
public class ClusterConfigInfo {
    public long currentEpoch;
    public long lastVoteEpoch;
    public Map<String, ClusterNodeInfo> nodes = new LinkedHashMap<>();
    public String[] migratingSlotsTo = new String[CLUSTER_SLOTS];
    public String[] importingSlotsFrom = new String[CLUSTER_SLOTS];

    public static ClusterConfigInfo valueOf(ClusterState state) {
        ClusterConfigInfo info = new ClusterConfigInfo();
        info.nodes = new LinkedHashMap<>();
        info.currentEpoch = state.currentEpoch;
        info.lastVoteEpoch = state.lastVoteEpoch;
        info.migratingSlotsTo = new String[CLUSTER_SLOTS];
        info.importingSlotsFrom = new String[CLUSTER_SLOTS];

        for (ClusterNode node : state.nodes.values()) {
            info.nodes.put(node.name, ClusterNodeInfo.valueOf(node, state.myself));
        }

        for (int i = 0; i < CLUSTER_SLOTS; i++) {
            if (state.migratingSlotsTo[i] != null) {
                info.migratingSlotsTo[i] = state.migratingSlotsTo[i].name;
            }
            if (state.importingSlotsFrom[i] != null) {
                info.importingSlotsFrom[i] = state.importingSlotsFrom[i].name;
            }
        }
        return info;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ClusterConfigInfo that = (ClusterConfigInfo) o;

        if (currentEpoch != that.currentEpoch) return false;
        if (lastVoteEpoch != that.lastVoteEpoch) return false;
        if (!nodes.equals(that.nodes)) return false;
        if (!Arrays.equals(migratingSlotsTo, that.migratingSlotsTo)) return false;
        return Arrays.equals(importingSlotsFrom, that.importingSlotsFrom);
    }

    @Override
    public int hashCode() {
        int result = (int) (currentEpoch ^ (currentEpoch >>> 32));
        result = 31 * result + (int) (lastVoteEpoch ^ (lastVoteEpoch >>> 32));
        result = 31 * result + nodes.hashCode();
        result = 31 * result + Arrays.hashCode(migratingSlotsTo);
        result = 31 * result + Arrays.hashCode(importingSlotsFrom);
        return result;
    }

    @Override
    public String toString() {
        return "Config:[" + "currentEpoch=" + currentEpoch + ", lastVoteEpoch=" + lastVoteEpoch + ", nodes=" + nodes + ']';
    }
}

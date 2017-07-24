package com.moilioncircle.redis.cluster.watchdog;

import com.moilioncircle.redis.cluster.watchdog.state.ClusterNode;
import com.moilioncircle.redis.cluster.watchdog.state.ClusterState;

import java.util.LinkedHashMap;
import java.util.Map;

import static com.moilioncircle.redis.cluster.watchdog.ClusterConstants.CLUSTER_SLOTS;

/**
 * @author Leon Chen
 * @since 1.0.0
 */
public class ConfigInfo {
    public long currentEpoch;
    public long lastVoteEpoch;
    public Map<String, NodeInfo> nodes = new LinkedHashMap<>();
    public String[] migratingSlotsTo = new String[CLUSTER_SLOTS];
    public String[] importingSlotsFrom = new String[CLUSTER_SLOTS];

    public static ConfigInfo valueOf(ClusterState state) {
        ConfigInfo info = new ConfigInfo();
        info.nodes = new LinkedHashMap<>();
        info.currentEpoch = state.currentEpoch;
        info.lastVoteEpoch = state.lastVoteEpoch;
        for (ClusterNode node : state.nodes.values()) {
            info.nodes.put(node.name, NodeInfo.valueOf(node, state.myself));
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

        ConfigInfo that = (ConfigInfo) o;

        if (currentEpoch != that.currentEpoch) return false;
        if (lastVoteEpoch != that.lastVoteEpoch) return false;
        return nodes.equals(that.nodes);
    }

    @Override
    public int hashCode() {
        int result = (int) (currentEpoch ^ (currentEpoch >>> 32));
        result = 31 * result + (int) (lastVoteEpoch ^ (lastVoteEpoch >>> 32));
        result = 31 * result + nodes.hashCode();
        return result;
    }
}

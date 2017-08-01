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
    private long currentEpoch; private long lastVoteEpoch;
    private String[] migrating = new String[CLUSTER_SLOTS];
    private String[] importing = new String[CLUSTER_SLOTS];
    private Map<String, ClusterNodeInfo> nodes = new LinkedHashMap<>();

    public static ClusterConfigInfo valueOf(ClusterState state) {
        ClusterConfigInfo info = new ClusterConfigInfo();
        info.nodes = new LinkedHashMap<>();
        info.currentEpoch = state.currentEpoch;
        info.lastVoteEpoch = state.lastVoteEpoch;
        info.migrating = new String[CLUSTER_SLOTS];
        info.importing = new String[CLUSTER_SLOTS];

        for (ClusterNode node : state.nodes.values()) {
            info.nodes.put(node.name, ClusterNodeInfo.valueOf(node, state.myself));
        }
        for (int i = 0; i < CLUSTER_SLOTS; i++) {
            if (state.migrating[i] != null) info.migrating[i] = state.migrating[i].name;
            if (state.importing[i] != null) info.importing[i] = state.importing[i].name;
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
        if (!Arrays.equals(migrating, that.migrating)) return false;
        return Arrays.equals(importing, that.importing);
    }

    @Override
    public int hashCode() {
        int result = (int) (currentEpoch ^ (currentEpoch >>> 32));
        result = 31 * result + (int) (lastVoteEpoch ^ (lastVoteEpoch >>> 32));
        result = 31 * result + nodes.hashCode();
        result = 31 * result + Arrays.hashCode(migrating);
        result = 31 * result + Arrays.hashCode(importing);
        return result;
    }

    /**
     *
     */
    public long getCurrentEpoch() {
        return currentEpoch;
    }

    public long getLastVoteEpoch() {
        return lastVoteEpoch;
    }

    public String[] getMigrating() {
        return migrating;
    }

    public String[] getImporting() {
        return importing;
    }

    public Map<String, ClusterNodeInfo> getNodes() {
        return nodes;
    }

    /**
     *
     */
    public void setMigrating(String[] migrating) {
        this.migrating = migrating;
    }

    public void setImporting(String[] importing) {
        this.importing = importing;
    }

    public void setCurrentEpoch(long currentEpoch) {
        this.currentEpoch = currentEpoch;
    }

    public void setLastVoteEpoch(long lastVoteEpoch) {
        this.lastVoteEpoch = lastVoteEpoch;
    }

    public void setNodes(Map<String, ClusterNodeInfo> nodes) {
        this.nodes = nodes;
    }

    @Override
    public String toString() {
        return "Config:[" + "currentEpoch=" + currentEpoch + ", lastVoteEpoch=" + lastVoteEpoch + ", nodes=" + nodes + ']';
    }
}

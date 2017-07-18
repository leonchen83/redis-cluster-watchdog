package com.moilioncircle.replicator.cluster.config;

import com.moilioncircle.replicator.cluster.ClusterNode;
import com.moilioncircle.replicator.cluster.ClusterState;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Created by Baoyi Chen on 2017/7/17.
 */
public class ConfigInfo {
    public long currentEpoch;
    public long lastVoteEpoch;
    public Map<String, NodeInfo> nodes = new LinkedHashMap<>();

    public static ConfigInfo valueOf(ClusterState state) {
        ConfigInfo info = new ConfigInfo();
        info.currentEpoch = state.currentEpoch;
        info.lastVoteEpoch = state.lastVoteEpoch;
        info.nodes = new LinkedHashMap<>();
        for (ClusterNode node : state.nodes.values()) {
            NodeInfo n = new NodeInfo();
            n.configEpoch = node.configEpoch;
            n.name = node.name;
            n.port = node.port;
            n.cport = node.cport;
            n.flags = node.flags;
            n.ip = node.ip;
            n.link = node.link != null || node.equals(state.myself) ? "connected" : "disconnected";
            n.slaveof = node.slaveof == null ? null : node.slaveof.name;
            System.arraycopy(node.slots, 0, n.slots, 0, node.slots.length);
            info.nodes.put(node.name, n);
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

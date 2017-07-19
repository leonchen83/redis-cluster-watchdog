package com.moilioncircle.replicator.cluster.config;

import com.moilioncircle.replicator.cluster.state.ClusterNode;

import java.util.Arrays;

import static com.moilioncircle.replicator.cluster.ClusterConstants.CLUSTER_SLOTS;

/**
 * Created by Baoyi Chen on 2017/7/17.
 */
public class NodeInfo {
    public int port;
    public int cport;
    public String ip;
    public int flags;
    public String name;
    public String link;
    public long pingSent;
    public String slaveof;
    public long configEpoch;
    public long pongReceived;
    public byte[] slots = new byte[CLUSTER_SLOTS / 8];

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        NodeInfo nodeInfo = (NodeInfo) o;

        if (port != nodeInfo.port) return false;
        if (cport != nodeInfo.cport) return false;
        if (flags != nodeInfo.flags) return false;
        if (configEpoch != nodeInfo.configEpoch) return false;
        if (!name.equals(nodeInfo.name)) return false;
        if (ip != null ? !ip.equals(nodeInfo.ip) : nodeInfo.ip != null) return false;
        if (slaveof != null ? !slaveof.equals(nodeInfo.slaveof) : nodeInfo.slaveof != null) return false;
        if (link != null ? !link.equals(nodeInfo.link) : nodeInfo.link != null) return false;
        return Arrays.equals(slots, nodeInfo.slots);
    }

    @Override
    public int hashCode() {
        int result = name.hashCode();
        result = 31 * result + (ip != null ? ip.hashCode() : 0);
        result = 31 * result + port;
        result = 31 * result + cport;
        result = 31 * result + flags;
        result = 31 * result + (slaveof != null ? slaveof.hashCode() : 0);
        result = 31 * result + (int) (configEpoch ^ (configEpoch >>> 32));
        result = 31 * result + (link != null ? link.hashCode() : 0);
        result = 31 * result + Arrays.hashCode(slots);
        return result;
    }

    public static NodeInfo valueOf(ClusterNode node, ClusterNode myself) {
        NodeInfo n = new NodeInfo();
        n.ip = node.ip;
        n.name = node.name;
        n.port = node.port;
        n.cport = node.cport;
        n.flags = node.flags;
        n.pingSent = node.pingSent;
        n.configEpoch = node.configEpoch;
        n.pongReceived = node.pongReceived;
        n.slaveof = node.slaveof == null ? null : node.slaveof.name;
        System.arraycopy(node.slots, 0, n.slots, 0, node.slots.length);
        n.link = node.link != null || node.equals(myself) ? "connected" : "disconnected";
        return n;
    }
}

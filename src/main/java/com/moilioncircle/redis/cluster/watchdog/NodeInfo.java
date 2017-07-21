package com.moilioncircle.redis.cluster.watchdog;

import com.moilioncircle.redis.cluster.watchdog.state.ClusterNode;

import java.util.Arrays;

import static com.moilioncircle.redis.cluster.watchdog.ClusterConstants.CLUSTER_SLOTS_BYTES;

/**
 * @author Leon Chen
 * @since 1.0.0
 */
public class NodeInfo {
    public int port;
    public String ip;
    public int flags;
    public int busPort;
    public String name;
    public String link;
    public long pingTime;
    public long pongTime;
    public String master;
    public long configEpoch;
    public byte[] slots = new byte[CLUSTER_SLOTS_BYTES];

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        NodeInfo nodeInfo = (NodeInfo) o;

        if (port != nodeInfo.port) return false;
        if (busPort != nodeInfo.busPort) return false;
        if (flags != nodeInfo.flags) return false;
        if (configEpoch != nodeInfo.configEpoch) return false;
        if (!name.equals(nodeInfo.name)) return false;
        if (ip != null ? !ip.equals(nodeInfo.ip) : nodeInfo.ip != null) return false;
        if (master != null ? !master.equals(nodeInfo.master) : nodeInfo.master != null) return false;
        if (link != null ? !link.equals(nodeInfo.link) : nodeInfo.link != null) return false;
        return Arrays.equals(slots, nodeInfo.slots);
    }

    @Override
    public int hashCode() {
        int result = name.hashCode();
        result = 31 * result + (ip != null ? ip.hashCode() : 0);
        result = 31 * result + port;
        result = 31 * result + busPort;
        result = 31 * result + flags;
        result = 31 * result + (master != null ? master.hashCode() : 0);
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
        n.flags = node.flags;
        n.busPort = node.busPort;
        n.pingTime = node.pingTime;
        n.pongTime = node.pongTime;
        n.configEpoch = node.configEpoch;
        n.master = node.master == null ? null : node.master.name;
        System.arraycopy(node.slots, 0, n.slots, 0, node.slots.length);
        n.link = node.link != null || node.equals(myself) ? "connected" : "disconnected";
        return n;
    }
}

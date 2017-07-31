package com.moilioncircle.redis.cluster.watchdog;

import com.moilioncircle.redis.cluster.watchdog.state.ClusterNode;

import java.util.Arrays;
import java.util.Objects;

import static com.moilioncircle.redis.cluster.watchdog.ClusterConstants.CLUSTER_SLOTS_BYTES;

/**
 * @author Leon Chen
 * @since 1.0.0
 */
public class ClusterNodeInfo {

    public long pingTime; public long pongTime;
    public String master; public long configEpoch;
    public byte[] slots = new byte[CLUSTER_SLOTS_BYTES];
    public String ip; public int port; public int busPort;
    public int flags; public String name; public String link;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ClusterNodeInfo nodeInfo = (ClusterNodeInfo) o;

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

    public static ClusterNodeInfo valueOf(ClusterNode myself) {
        return valueOf(myself, myself);
    }

    public static ClusterNodeInfo valueOf(ClusterNode node, ClusterNode myself) {
        ClusterNodeInfo n = new ClusterNodeInfo();
        n.configEpoch = node.configEpoch;
        n.name = node.name; n.flags = node.flags;
        n.pingTime = node.pingTime; n.pongTime = node.pongTime;
        n.master = node.master == null ? null : node.master.name;
        n.ip = node.ip; n.port = node.port; n.busPort = node.busPort;
        System.arraycopy(node.slots, 0, n.slots, 0, node.slots.length);
        n.link = node.link != null || Objects.equals(node, myself) ? "connected" : "disconnected";
        return n;
    }

    @Override
    public String toString() {
        return "Node:[" +
                "address='" + (ip == null ? "0.0.0.0" : ip) + ":" + port + "@" + busPort + '\'' +
                ", flags=" + flags + ", name='" + name + '\'' + ", link='" + link + '\'' +
                ", pingTime=" + pingTime + ", pongTime=" + pongTime + ", master='" + master + '\'' +
                ", configEpoch=" + configEpoch + ']';
    }
}

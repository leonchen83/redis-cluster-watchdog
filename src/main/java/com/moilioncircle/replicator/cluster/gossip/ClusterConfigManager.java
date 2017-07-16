package com.moilioncircle.replicator.cluster.gossip;

import com.moilioncircle.replicator.cluster.ClusterNode;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.AbstractMap;
import java.util.Map;

import static com.moilioncircle.replicator.cluster.ClusterConstants.*;

/**
 * Created by Baoyi Chen on 2017/7/12.
 */
public class ClusterConfigManager {
    private static final Log logger = LogFactory.getLog(ClusterConfigManager.class);
    private Server server;
    private ThinGossip gossip;

    public ClusterConfigManager(ThinGossip gossip) {
        this.gossip = gossip;
        this.server = gossip.server;
    }

    public static Map.Entry<Integer, String>[] redisNodeFlags = new Map.Entry[]{
            new AbstractMap.SimpleEntry<>(CLUSTER_NODE_MYSELF, "myself,"),
            new AbstractMap.SimpleEntry<>(CLUSTER_NODE_MASTER, "master,"),
            new AbstractMap.SimpleEntry<>(CLUSTER_NODE_SLAVE, "slave,"),
            new AbstractMap.SimpleEntry<>(CLUSTER_NODE_PFAIL, "fail?,"),
            new AbstractMap.SimpleEntry<>(CLUSTER_NODE_FAIL, "fail,"),
            new AbstractMap.SimpleEntry<>(CLUSTER_NODE_HANDSHAKE, "handshake,"),
            new AbstractMap.SimpleEntry<>(CLUSTER_NODE_NOADDR, "noaddr,"),
    };

    public String representClusterNodeFlags(int flags) {
        StringBuilder builder = new StringBuilder();
        if (flags == 0) {
            builder.append("noflags,");
        } else {
            for (Map.Entry<Integer, String> rnf : redisNodeFlags) {
                if ((flags & rnf.getKey()) != 0) builder.append(rnf.getValue());
            }
        }
        builder.deleteCharAt(builder.length() - 1);
        return builder.toString();
    }

    public String clusterGenNodeDescription(ClusterNode node) {
        StringBuilder ci = new StringBuilder();

        ci.append(node.name).append(" ").append(node.ip == null ? "0.0.0.0" : node.ip).append(":").append(node.port).append("@").append(node.cport).append(" ");
        ci.append(representClusterNodeFlags(node.flags));
        if (node.slaveof != null)
            ci.append(" ").append(node.slaveof.name).append(" ");
        else
            ci.append(" - ");

        ci.append(node.pingSent).append(" ").append(node.pongReceived).append(" ").append(node.configEpoch).append(" ").append((node.link != null || (node.flags & CLUSTER_NODE_MYSELF) != 0) ? "connected" : "disconnected");

        int start = -1;
        for (int i = 0; i < CLUSTER_SLOTS; i++) {
            boolean bit;

            if ((bit = gossip.slotManger.clusterNodeGetSlotBit(node, i))) {
                if (start == -1) start = i;
            }
            if (start != -1 && (!bit || i == CLUSTER_SLOTS - 1)) {
                if (bit && i == CLUSTER_SLOTS - 1) i++;
                if (start == i - 1) {
                    ci.append(" ").append(start);
                } else {
                    ci.append(" ").append(start).append("-").append(i - 1);
                }
                start = -1;
            }
        }

        return ci.toString();
    }

    public String clusterGenNodesDescription() {
        StringBuilder ci = new StringBuilder();
        for (ClusterNode node : server.cluster.nodes.values()) {
            ci.append(clusterGenNodeDescription(node));
            ci.append("\n");
        }
        return ci.toString();
    }

    public String clusterGetMessageTypeString(int type) {
        switch (type) {
            case CLUSTERMSG_TYPE_PING:
                return "ping";
            case CLUSTERMSG_TYPE_PONG:
                return "pong";
            case CLUSTERMSG_TYPE_MEET:
                return "meet";
            case CLUSTERMSG_TYPE_FAIL:
                return "fail";
            case CLUSTERMSG_TYPE_PUBLISH:
                return "publish";
            case CLUSTERMSG_TYPE_FAILOVER_AUTH_REQUEST:
                return "auth-req";
            case CLUSTERMSG_TYPE_FAILOVER_AUTH_ACK:
                return "auth-ack";
            case CLUSTERMSG_TYPE_UPDATE:
                return "update";
            case CLUSTERMSG_TYPE_MFSTART:
                return "mfstart";
        }
        return "unknown";
    }
}

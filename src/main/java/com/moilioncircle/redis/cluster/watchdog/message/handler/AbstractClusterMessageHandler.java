package com.moilioncircle.redis.cluster.watchdog.message.handler;

import com.moilioncircle.redis.cluster.watchdog.ClusterConfiguration;
import com.moilioncircle.redis.cluster.watchdog.manager.ClusterManagers;
import com.moilioncircle.redis.cluster.watchdog.message.ClusterMessage;
import com.moilioncircle.redis.cluster.watchdog.message.ClusterMessageDataGossip;
import com.moilioncircle.redis.cluster.watchdog.state.ClusterLink;
import com.moilioncircle.redis.cluster.watchdog.state.ClusterNode;
import com.moilioncircle.redis.cluster.watchdog.state.ServerState;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.List;
import java.util.Objects;

import static com.moilioncircle.redis.cluster.watchdog.ClusterConstants.*;
import static com.moilioncircle.redis.cluster.watchdog.ClusterNodeInfo.valueOf;
import static com.moilioncircle.redis.cluster.watchdog.Version.PROTOCOL_V1;
import static com.moilioncircle.redis.cluster.watchdog.manager.ClusterConfigManager.representClusterNodeFlags;
import static com.moilioncircle.redis.cluster.watchdog.manager.ClusterSlotManger.bitmapTestBit;
import static com.moilioncircle.redis.cluster.watchdog.state.NodeStates.*;
import static java.lang.Math.max;

/**
 * @author Leon Chen
 * @since 1.0.0
 */
public abstract class AbstractClusterMessageHandler implements ClusterMessageHandler {

    private static final Log logger = LogFactory.getLog(AbstractClusterMessageHandler.class);

    protected ServerState server;
    protected ClusterManagers managers;
    protected ClusterConfiguration configuration;

    public AbstractClusterMessageHandler(ClusterManagers managers) {
        this.managers = managers;
        this.server = managers.server;
        this.configuration = managers.configuration;
    }

    public abstract boolean handle(ClusterNode sender, ClusterLink link, ClusterMessage hdr);

    @Override
    public boolean handle(ClusterLink link, ClusterMessage hdr) {
        if (hdr.type < CLUSTERMSG_TYPE_COUNT)
            server.cluster.messagesReceived[hdr.type]++;
        if (hdr.version != configuration.getVersion()) return true;
        ClusterNode sender = managers.nodes.clusterLookupNode(hdr.name);
        if (sender != null && !nodeInHandshake(sender)) {
            sender.configEpoch = max(hdr.configEpoch, sender.configEpoch);
            server.cluster.currentEpoch = max(hdr.currentEpoch, server.cluster.currentEpoch);
        }
        handle(sender, link, hdr); managers.states.clusterUpdateState(); return true;
    }

    public void clusterUpdateSlotsConfigWith(ClusterNode sender, long senderConfigEpoch, byte[] slots) {
        ClusterNode previous = nodeIsMaster(server.myself) ? server.myself : server.myself.master;
        if (Objects.equals(sender, server.myself)) { logger.info("Discarding UPDATE message fail myself."); return; }

        ClusterNode next = null;
        for (int i = 0; i < CLUSTER_SLOTS; i++) {
            ClusterNode s = server.cluster.slots[i];
            if (!bitmapTestBit(slots, i)) continue;
            if (Objects.equals(s, sender)) continue;
            if (server.cluster.importing[i] != null) continue;
            if (s == null || s.configEpoch < senderConfigEpoch) {
                if (Objects.equals(s, previous)) next = sender;
                managers.slots.clusterDelSlot(i); managers.slots.clusterAddSlot(sender, i);
            }
        }
        if (next != null && previous.assignedSlots == 0) managers.nodes.clusterSetMyMasterTo(sender);
    }

    public void clusterProcessGossipSection(ClusterMessage hdr, ClusterLink link) {
        List<ClusterMessageDataGossip> gossips = hdr.data.gossips;
        ClusterNode sender = link.node != null ? link.node : managers.nodes.clusterLookupNode(hdr.name);
        for (ClusterMessageDataGossip gossip : gossips) {
            if (logger.isDebugEnabled()) {
                logger.debug("GOSSIP " + gossip.name + " " + gossip.ip + ":" + gossip.port + "@" + gossip.busPort + " " + representClusterNodeFlags(gossip.flags));
            }

            ClusterNode node = managers.nodes.clusterLookupNode(gossip.name);

            if (node == null) {
                if (sender != null && (gossip.flags & CLUSTER_NODE_NOADDR) == 0
                        && !managers.blacklists.clusterBlacklistExists(gossip.name)) {
                    managers.nodes.clusterStartHandshake(gossip.ip, gossip.port, gossip.busPort);
                }
                continue;
            }

            if (sender != null && nodeIsMaster(sender) && !Objects.equals(node, server.myself)) {
                if ((gossip.flags & (CLUSTER_NODE_FAIL | CLUSTER_NODE_PFAIL)) != 0) {
                    if (managers.nodes.clusterNodeAddFailureReport(node, sender) && configuration.isVerbose()) {
                        logger.info("Node " + sender.name + " reported node " + node.name + " as not reachable.");
                    }
                    markNodeAsFailingIfNeeded(node);
                } else if (managers.nodes.clusterNodeDelFailureReport(node, sender) && configuration.isVerbose()) {
                    logger.info("Node " + sender.name + " reported node " + node.name + " is back online.");
                }
            }

            if (configuration.getVersion() == PROTOCOL_V1
                    && (gossip.flags & (CLUSTER_NODE_FAIL | CLUSTER_NODE_PFAIL)) == 0
                    && node.pingTime == 0 && managers.nodes.clusterNodeFailureReportsCount(node) == 0
                    && gossip.pongTime <= (System.currentTimeMillis() + 500) && gossip.pongTime > node.pongTime) {
                node.pongTime = gossip.pongTime;
            }

            if ((node.flags & (CLUSTER_NODE_FAIL | CLUSTER_NODE_PFAIL)) != 0
                    && (gossip.flags & (CLUSTER_NODE_NOADDR | CLUSTER_NODE_FAIL | CLUSTER_NODE_PFAIL)) == 0
                    && (!node.ip.equalsIgnoreCase(gossip.ip) || node.port != gossip.port || node.busPort != gossip.busPort)) {

                if (node.link != null) managers.connections.freeClusterLink(node.link);
                node.ip = gossip.ip; node.port = gossip.port; node.busPort = gossip.busPort;
                node.flags &= ~CLUSTER_NODE_NOADDR;
            }
        }
    }

    public boolean nodeUpdateAddressIfNeeded(ClusterNode node, ClusterLink link, ClusterMessage hdr) {
        if (link.equals(node.link)) return false;
        String ip = link.fd.getRemoteAddress(hdr.ip);
        if (node.port == hdr.port && node.busPort == hdr.busPort && ip.equalsIgnoreCase(node.ip)) return false;
        node.ip = ip; node.port = hdr.port; node.busPort = hdr.busPort;
        if (node.link != null) managers.connections.freeClusterLink(node.link);
        logger.info("Address updated for node " + node.name + ", now " + node.ip + ":" + node.port);
        if (nodeIsSlave(server.myself) && Objects.equals(server.myself.master, node)) {
            managers.replications.replicationSetMaster(node);
        }
        return true;
    }

    public void markNodeAsFailingIfNeeded(ClusterNode node) {
        int quorum = server.cluster.size / 2 + 1;
        if (!nodePFailed(node) || nodeFailed(node)) return;
        int failures = managers.nodes.clusterNodeFailureReportsCount(node);
        if (nodeIsMaster(server.myself)) failures++; if (failures < quorum) return;
        logger.info("Marking node " + node.name + " as failing (quorum reached).");
        //
        long now = System.currentTimeMillis();
        node.flags &= ~CLUSTER_NODE_PFAIL; node.flags |= CLUSTER_NODE_FAIL;
        node.failTime = now; managers.notifyNodeFailed(valueOf(node, server.myself));
        if (nodeIsMaster(server.myself)) managers.messages.clusterSendFail(node.name);
    }

    public void clusterHandleConfigEpochCollision(ClusterNode sender) {
        if (sender.configEpoch != server.myself.configEpoch) return;
        if (nodeIsSlave(sender) || nodeIsSlave(server.myself)) return;
        if (sender.name.compareTo(server.myself.name) <= 0) return;
        server.cluster.currentEpoch++; server.myself.configEpoch = server.cluster.currentEpoch;
        logger.info("WARNING: configEpoch collision with node " + sender.name + ". configEpoch set to " + server.myself.configEpoch);
    }
}

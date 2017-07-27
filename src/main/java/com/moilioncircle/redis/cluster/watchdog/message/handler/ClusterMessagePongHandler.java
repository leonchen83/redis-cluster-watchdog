package com.moilioncircle.redis.cluster.watchdog.message.handler;

import com.moilioncircle.redis.cluster.watchdog.manager.ClusterManagers;
import com.moilioncircle.redis.cluster.watchdog.message.ClusterMessage;
import com.moilioncircle.redis.cluster.watchdog.state.ClusterLink;
import com.moilioncircle.redis.cluster.watchdog.state.ClusterNode;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.Arrays;
import java.util.Objects;

import static com.moilioncircle.redis.cluster.watchdog.ClusterConstants.*;
import static com.moilioncircle.redis.cluster.watchdog.ClusterNodeInfo.valueOf;
import static com.moilioncircle.redis.cluster.watchdog.manager.ClusterSlotManger.bitmapTestBit;
import static com.moilioncircle.redis.cluster.watchdog.state.NodeStates.*;

/**
 * @author Leon Chen
 * @since 1.0.0
 */
public class ClusterMessagePongHandler extends AbstractClusterMessageHandler {

    private static final Log logger = LogFactory.getLog(ClusterMessagePongHandler.class);

    public ClusterMessagePongHandler(ClusterManagers managers) {
        super(managers);
    }

    @Override
    public boolean handle(ClusterNode sender, ClusterLink link, ClusterMessage hdr) {
        logger.debug("Pong packet received: node:" + (link.node == null ? "(nil)" : link.node.name));

        if (link.node != null) {
            if (nodeInHandshake(link.node)) {
                if (sender != null) {
                    if (managers.configuration.isVerbose())
                        logger.info("Handshake: we already know node " + sender.name + ", updating the address if needed.");
                    nodeUpdateAddressIfNeeded(sender, link, hdr);
                    managers.nodes.clusterDelNode(link.node);
                    return false;
                }

                managers.nodes.clusterRenameNode(link.node, hdr.name);
                if (managers.configuration.isVerbose())
                    logger.info("Handshake with node " + link.node.name + " completed.");
                link.node.flags &= ~CLUSTER_NODE_HANDSHAKE;
                link.node.flags |= hdr.flags & (CLUSTER_NODE_MASTER | CLUSTER_NODE_SLAVE);
            } else if (!link.node.name.equals(hdr.name)) {
                logger.debug("PONG contains mismatching name ID. About node " + link.node.name + " added " + (System.currentTimeMillis() - link.node.createTime) + " ms ago, having flags " + link.node.flags);
                link.node.flags |= CLUSTER_NODE_NOADDR;
                link.node.ip = null;
                link.node.port = 0;
                link.node.busPort = 0;
                managers.connections.freeClusterLink(link);
                return false;
            }

            link.node.pongTime = System.currentTimeMillis();
            link.node.pingTime = 0;

            if (nodePFailed(link.node)) {
                link.node.flags &= ~CLUSTER_NODE_PFAIL;
                managers.notifyUnsetNodePFailed(valueOf(link.node, server.myself));
            } else if (nodeFailed(link.node)) {
                clearNodeFailureIfNeeded(link.node);
            }
        }

        if (sender == null) return true;

        if (hdr.master == null) {
            managers.nodes.clusterSetNodeAsMaster(sender);
        } else {
            ClusterNode master = managers.nodes.clusterLookupNode(hdr.master);

            if (nodeIsMaster(sender)) {
                managers.slots.clusterDelNodeSlots(sender);
                sender.flags &= ~(CLUSTER_NODE_MASTER | CLUSTER_NODE_MIGRATE_TO);
                sender.flags |= CLUSTER_NODE_SLAVE;
            }

            if (master != null && (sender.master == null || !Objects.equals(sender.master, master))) {
                if (sender.master != null)
                    managers.nodes.clusterNodeRemoveSlave(sender.master, sender);
                managers.nodes.clusterNodeAddSlave(master, sender);
                sender.master = master;
            }
        }

        ClusterNode senderMaster = nodeIsMaster(sender) ? sender : sender.master;

        if (senderMaster != null && !Arrays.equals(senderMaster.slots, hdr.slots)) {
            if (nodeIsMaster(sender))
                clusterUpdateSlotsConfigWith(sender, hdr.configEpoch, hdr.slots);

            for (int i = 0; i < CLUSTER_SLOTS; i++) {
                if (!bitmapTestBit(hdr.slots, i)) continue;
                if (server.cluster.slots[i] == null) continue;
                if (Objects.equals(server.cluster.slots[i], sender)) continue;
                if (server.cluster.slots[i].configEpoch <= hdr.configEpoch) continue;
                if (managers.configuration.isVerbose())
                    logger.info("Node " + sender.name + " has old slots configuration, sending an UPDATE message fail " + server.cluster.slots[i].name);
                managers.messages.clusterSendUpdate(sender.link, server.cluster.slots[i]);
                break;
            }
        }

        if (nodeIsMaster(server.myself) && nodeIsMaster(sender) && hdr.configEpoch == server.myself.configEpoch) {
            clusterHandleConfigEpochCollision(sender);
        }

        clusterProcessGossipSection(hdr, link);
        return true;
    }

    public void clearNodeFailureIfNeeded(ClusterNode node) {
        long now = System.currentTimeMillis();

        if (nodeIsSlave(node) || node.assignedSlots == 0) {
            logger.info("Clear FAIL state for node " + node.name + ": " + (nodeIsSlave(node) ? "slave" : "master without slots") + " is reachable again.");
            node.flags &= ~CLUSTER_NODE_FAIL;
            managers.notifyUnsetNodeFailed(valueOf(node, server.myself));
        }

        if (nodeIsMaster(node) && node.assignedSlots > 0 && now - node.failTime > managers.configuration.getClusterNodeTimeout() * CLUSTER_FAIL_UNDO_TIME_MULTI) {
            logger.info("Clear FAIL state for node " + node.name + ": is reachable again and nobody is serving its slots after some createTime.");
            node.flags &= ~CLUSTER_NODE_FAIL;
            managers.notifyUnsetNodeFailed(valueOf(node, server.myself));
        }
    }
}

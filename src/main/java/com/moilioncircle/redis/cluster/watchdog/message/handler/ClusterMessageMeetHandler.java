package com.moilioncircle.redis.cluster.watchdog.message.handler;

import com.moilioncircle.redis.cluster.watchdog.manager.ClusterManagers;
import com.moilioncircle.redis.cluster.watchdog.message.ClusterMessage;
import com.moilioncircle.redis.cluster.watchdog.state.ClusterLink;
import com.moilioncircle.redis.cluster.watchdog.state.ClusterNode;

import java.util.Arrays;

import static com.moilioncircle.redis.cluster.watchdog.ClusterConstants.*;
import static com.moilioncircle.redis.cluster.watchdog.manager.ClusterSlotManger.bitmapTestBit;
import static com.moilioncircle.redis.cluster.watchdog.state.States.nodeInHandshake;
import static com.moilioncircle.redis.cluster.watchdog.state.States.nodeIsMaster;

/**
 * @author Leon Chen
 * @since 1.0.0
 */
public class ClusterMessageMeetHandler extends AbstractClusterMessageHandler {
    public ClusterMessageMeetHandler(ClusterManagers managers) {
        super(managers);
    }

    @Override
    public boolean handle(ClusterNode sender, ClusterLink link, ClusterMessage hdr) {
        if (logger.isDebugEnabled()) {
            logger.debug("Meet packet received: node:" + link.node + ",name:" + sender + ",message:" + hdr);
        }

        if (server.myself.ip == null && managers.configuration.getClusterAnnounceIp() == null) {
            String ip = link.fd.getLocalAddress(null);
            if (ip != null && !ip.equals(server.myself.ip)) {
                server.myself.ip = ip;
                logger.info("IP address for this node updated to " + server.myself.ip);
            }
        }

        if (sender == null) {
            ClusterNode node = managers.nodes.createClusterNode(null, CLUSTER_NODE_HANDSHAKE);
            node.ip = link.fd.getRemoteAddress(hdr.ip);
            node.port = hdr.port;
            node.busPort = hdr.busPort;
            managers.nodes.clusterAddNode(node);
            clusterProcessGossipSection(hdr, link);
        }

        managers.messages.clusterSendPing(link, CLUSTERMSG_TYPE_PONG);

        if (link.node != null && nodeInHandshake(link.node)) {
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
        } else if (link.node != null && !link.node.name.equals(hdr.name)) {
            logger.debug("PONG contains mismatching name ID. About node " + link.node.name + " added " + (System.currentTimeMillis() - link.node.createTime) + " ms ago, having flags " + link.node.flags);
            link.node.flags |= CLUSTER_NODE_NOADDR;
            link.node.ip = null;
            link.node.port = 0;
            link.node.busPort = 0;
            managers.connections.freeClusterLink(link);
            return false;
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

            if (master != null && (sender.master == null || !sender.master.equals(master))) {
                if (sender.master != null) managers.nodes.clusterNodeRemoveSlave(sender.master, sender);
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
                if (server.cluster.slots[i] == null || server.cluster.slots[i].equals(sender)) continue;
                if (server.cluster.slots[i].configEpoch > hdr.configEpoch) {
                    if (managers.configuration.isVerbose())
                        logger.info("Node " + sender.name + " has old slots configuration, sending an UPDATE message fail " + server.cluster.slots[i].name);
                    managers.messages.clusterSendUpdate(sender.link, server.cluster.slots[i]);
                    break;
                }
            }
        }

        if (nodeIsMaster(server.myself) && nodeIsMaster(sender) && hdr.configEpoch == server.myself.configEpoch) {
            clusterHandleConfigEpochCollision(sender);
        }

        clusterProcessGossipSection(hdr, link);
        return true;
    }
}
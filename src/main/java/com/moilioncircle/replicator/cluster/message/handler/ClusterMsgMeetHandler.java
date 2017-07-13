package com.moilioncircle.replicator.cluster.message.handler;

import com.moilioncircle.replicator.cluster.ClusterLink;
import com.moilioncircle.replicator.cluster.ClusterNode;
import com.moilioncircle.replicator.cluster.gossip.ThinGossip1;
import com.moilioncircle.replicator.cluster.message.ClusterMsg;

import java.util.Arrays;

import static com.moilioncircle.replicator.cluster.ClusterConstants.*;

/**
 * Created by Baoyi Chen on 2017/7/13.
 */
public class ClusterMsgMeetHandler extends AbstractClusterMsgHandler {
    public ClusterMsgMeetHandler(ThinGossip1 gossip) {
        super(gossip);
    }

    @Override
    public boolean handle(ClusterNode sender, ClusterLink link, ClusterMsg hdr) {
        logger.debug("Meet packet received: " + link.node);
        if (myself.ip == null && server.clusterAnnounceIp == null) {
            String ip;
            if ((ip = link.fd.getLocalAddress()) != null && !ip.equals(myself.ip)) {
                myself.ip = ip;
                logger.warn("IP address for this node updated to " + myself.ip);
                gossip.clusterDoBeforeSleep(CLUSTER_TODO_SAVE_CONFIG);
            }
        }

        if (sender == null) {
            ClusterNode node = gossip.nodeManager.createClusterNode(null, CLUSTER_NODE_HANDSHAKE);
            node.ip = gossip.nodeIp2String(link, hdr.myip);
            node.port = hdr.port;
            node.cport = hdr.cport;
            gossip.nodeManager.clusterAddNode(node);
            gossip.clusterDoBeforeSleep(CLUSTER_TODO_SAVE_CONFIG);
            gossip.clusterProcessGossipSection(hdr, link);
        }

        gossip.msgManager.clusterSendPing(link, CLUSTERMSG_TYPE_PONG);

        if (link.node != null) {
            if (nodeInHandshake(link.node)) {
                if (sender != null) {
                    logger.debug("Handshake: we already know node " + sender.name + ", updating the address if needed.");
                    if (gossip.nodeUpdateAddressIfNeeded(sender, link, hdr)) {
                        gossip.clusterDoBeforeSleep(CLUSTER_TODO_SAVE_CONFIG | CLUSTER_TODO_UPDATE_STATE);
                    }
                    gossip.nodeManager.clusterDelNode(link.node);
                    return false;
                }

                gossip.nodeManager.clusterRenameNode(link.node, hdr.sender);
                logger.debug("Handshake with node " + link.node.name + " completed.");
                link.node.flags &= ~CLUSTER_NODE_HANDSHAKE;
                link.node.flags |= hdr.flags & (CLUSTER_NODE_MASTER | CLUSTER_NODE_SLAVE);
                gossip.clusterDoBeforeSleep(CLUSTER_TODO_SAVE_CONFIG);
            } else if (!link.node.name.equals(hdr.sender)) {
                logger.debug("PONG contains mismatching sender ID. About node " + link.node.name + " added " + (System.currentTimeMillis() - link.node.ctime) + " ms ago, having flags " + link.node.flags);
                link.node.flags |= CLUSTER_NODE_NOADDR;
                link.node.ip = null;
                link.node.port = 0;
                link.node.cport = 0;
                gossip.connectionManager.freeClusterLink(link);
                gossip.clusterDoBeforeSleep(CLUSTER_TODO_SAVE_CONFIG);
                return false;
            }
        }

        if (sender != null) {
            if (hdr.slaveof.equals(CLUSTER_NODE_NULL_NAME)) { // hdr.slaveof == null
                gossip.clusterSetNodeAsMaster(sender);
            } else {
                ClusterNode master = gossip.nodeManager.clusterLookupNode(hdr.slaveof);

                if (nodeIsMaster(sender)) {
                    gossip.slotManger.clusterDelNodeSlots(sender);
                    sender.flags &= ~(CLUSTER_NODE_MASTER | CLUSTER_NODE_MIGRATE_TO);
                    sender.flags |= CLUSTER_NODE_SLAVE;

                    gossip.clusterDoBeforeSleep(CLUSTER_TODO_SAVE_CONFIG | CLUSTER_TODO_UPDATE_STATE);
                }

                if (master != null && !sender.slaveof.equals(master)) {
                    if (sender.slaveof != null) gossip.nodeManager.clusterNodeRemoveSlave(sender.slaveof, sender);
                    gossip.nodeManager.clusterNodeAddSlave(master, sender);
                    sender.slaveof = master;
                    gossip.clusterDoBeforeSleep(CLUSTER_TODO_SAVE_CONFIG);
                }
            }
        }

        ClusterNode senderMaster = null;
        boolean dirtySlots = false;

        if (sender != null) {
            senderMaster = nodeIsMaster(sender) ? sender : sender.slaveof;
            if (senderMaster != null) {
                dirtySlots = !Arrays.equals(senderMaster.slots, hdr.myslots);
            }
        }

        if (sender != null && nodeIsMaster(sender) && dirtySlots)
            gossip.clusterUpdateSlotsConfigWith(sender, hdr.configEpoch, hdr.myslots);

        if (sender != null && dirtySlots) {
            for (int i = 0; i < CLUSTER_SLOTS; i++) {
                if (gossip.slotManger.bitmapTestBit(hdr.myslots, i)) {
                    if (server.cluster.slots[i].equals(sender) || server.cluster.slots[i] == null) continue;
                    if (server.cluster.slots[i].configEpoch > hdr.configEpoch) {
                        logger.debug("Node " + sender.name + " has old slots configuration, sending an UPDATE message about " + server.cluster.slots[i].name);
                        gossip.msgManager.clusterSendUpdate(sender.link, server.cluster.slots[i]);
                        break;
                    }
                }
            }
        }

        if (sender != null && nodeIsMaster(myself) && nodeIsMaster(sender) && hdr.configEpoch == myself.configEpoch) {
            gossip.clusterHandleConfigEpochCollision(sender);
        }

        if (sender != null) gossip.clusterProcessGossipSection(hdr, link);
        return true;
    }
}

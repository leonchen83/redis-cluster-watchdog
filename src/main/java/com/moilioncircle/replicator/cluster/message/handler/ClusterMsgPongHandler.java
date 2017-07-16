package com.moilioncircle.replicator.cluster.message.handler;

import com.moilioncircle.replicator.cluster.ClusterLink;
import com.moilioncircle.replicator.cluster.ClusterNode;
import com.moilioncircle.replicator.cluster.gossip.ThinGossip;
import com.moilioncircle.replicator.cluster.message.ClusterMsg;

import java.util.Arrays;

import static com.moilioncircle.replicator.cluster.ClusterConstants.*;
import static com.moilioncircle.replicator.cluster.gossip.ClusterSlotManger.bitmapTestBit;

/**
 * Created by Baoyi Chen on 2017/7/13.
 */
public class ClusterMsgPongHandler extends AbstractClusterMsgHandler {
    public ClusterMsgPongHandler(ThinGossip gossip) {
        super(gossip);
    }

    @Override
    public boolean handle(ClusterNode sender, ClusterLink link, ClusterMsg hdr) {
        logger.debug("Pong packet received: " + Thread.currentThread() + ",node:" + link.node + ",sender:" + sender + ",message:" + hdr);
        if (link.node != null) {
            if (nodeInHandshake(link.node)) {
                if (sender != null) {
                    logger.debug("Handshake: we already know node " + sender.name + ", updating the address if needed.");
                    gossip.nodeUpdateAddressIfNeeded(sender, link, hdr);
                    gossip.nodeManager.clusterDelNode(link.node);
                    return false;
                }

                gossip.nodeManager.clusterRenameNode(link.node, hdr.sender);
                logger.debug("Handshake with node " + link.node.name + " completed.");
                link.node.flags &= ~CLUSTER_NODE_HANDSHAKE;
                link.node.flags |= hdr.flags & (CLUSTER_NODE_MASTER | CLUSTER_NODE_SLAVE);
            } else if (!link.node.name.equals(hdr.sender)) {
                logger.debug("PONG contains mismatching sender ID. About node " + link.node.name + " added " + (System.currentTimeMillis() - link.node.ctime) + " ms ago, having flags " + link.node.flags);
                link.node.flags |= CLUSTER_NODE_NOADDR;
                link.node.ip = null;
                link.node.port = 0;
                link.node.cport = 0;
                gossip.connectionManager.freeClusterLink(link);
                return false;
            }

            link.node.pongReceived = System.currentTimeMillis();
            link.node.pingSent = 0;

            if (nodePFailed(link.node)) {
                link.node.flags &= ~CLUSTER_NODE_PFAIL;
            } else if (nodeFailed(link.node)) {
                gossip.clearNodeFailureIfNeeded(link.node);
            }
        }

        if (sender == null) return true;

        if (hdr.slaveof == null) { // hdr.slaveof == null
            gossip.clusterSetNodeAsMaster(sender);
        } else {
            ClusterNode master = gossip.nodeManager.clusterLookupNode(hdr.slaveof);

            if (nodeIsMaster(sender)) {
                gossip.slotManger.clusterDelNodeSlots(sender);
                sender.flags &= ~(CLUSTER_NODE_MASTER | CLUSTER_NODE_MIGRATE_TO);
                sender.flags |= CLUSTER_NODE_SLAVE;
            }

            if (master != null && !sender.slaveof.equals(master)) {
                if (sender.slaveof != null) gossip.nodeManager.clusterNodeRemoveSlave(sender.slaveof, sender);
                gossip.nodeManager.clusterNodeAddSlave(master, sender);
                sender.slaveof = master;
            }
        }

        boolean dirtySlots = false;
        ClusterNode senderMaster = nodeIsMaster(sender) ? sender : sender.slaveof;
        if (senderMaster != null) {
            dirtySlots = !Arrays.equals(senderMaster.slots, hdr.myslots);
        }

        if (nodeIsMaster(sender) && dirtySlots) {
            gossip.clusterUpdateSlotsConfigWith(sender, hdr.configEpoch, hdr.myslots);
        }

        if (dirtySlots) {
            for (int i = 0; i < CLUSTER_SLOTS; i++) {
                if (!bitmapTestBit(hdr.myslots, i)) continue;
                if (server.cluster.slots[i] == null || server.cluster.slots[i].equals(sender)) continue;
                if (server.cluster.slots[i].configEpoch > hdr.configEpoch) {
                    logger.debug("Node " + sender.name + " has old slots configuration, sending an UPDATE message about " + server.cluster.slots[i].name);
                    gossip.msgManager.clusterSendUpdate(sender.link, server.cluster.slots[i]);
                    break;
                }
            }
        }

        if (nodeIsMaster(server.myself) && nodeIsMaster(sender) && hdr.configEpoch == server.myself.configEpoch) {
            gossip.clusterHandleConfigEpochCollision(sender);
        }

        gossip.clusterProcessGossipSection(hdr, link);
        return true;
    }
}

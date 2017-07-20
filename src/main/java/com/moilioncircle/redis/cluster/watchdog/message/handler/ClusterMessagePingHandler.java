package com.moilioncircle.redis.cluster.watchdog.message.handler;

import com.moilioncircle.redis.cluster.watchdog.manager.ClusterManagers;
import com.moilioncircle.redis.cluster.watchdog.message.ClusterMessage;
import com.moilioncircle.redis.cluster.watchdog.state.ClusterLink;
import com.moilioncircle.redis.cluster.watchdog.state.ClusterNode;
import com.moilioncircle.redis.cluster.watchdog.state.States;

import java.util.Arrays;

import static com.moilioncircle.redis.cluster.watchdog.ClusterConstants.*;
import static com.moilioncircle.redis.cluster.watchdog.manager.ClusterSlotManger.bitmapTestBit;

/**
 * Created by Baoyi Chen on 2017/7/13.
 */
public class ClusterMessagePingHandler extends AbstractClusterMessageHandler {
    public ClusterMessagePingHandler(ClusterManagers gossip) {
        super(gossip);
    }

    @Override
    public boolean handle(ClusterNode sender, ClusterLink link, ClusterMessage hdr) {
        logger.debug("Ping packet received: " + Thread.currentThread() + ",node:" + link.node + ",sender:" + sender + ",message:" + hdr);
        if (server.myself.ip == null && managers.configuration.getClusterAnnounceIp() == null) {
            String ip = link.fd.getLocalAddress(null);
            if (ip != null && !ip.equals(server.myself.ip)) {
                server.myself.ip = ip;
                logger.info("IP address for this node updated to " + server.myself.ip);
            }
        }

        managers.messages.clusterSendPing(link, CLUSTERMSG_TYPE_PONG);

        if (link.node != null && States.nodeInHandshake(link.node)) {
            if (sender != null) {
                if (managers.configuration.isVerbose())
                    logger.info("Handshake: we already know node " + sender.name + ", updating the address if needed.");
                nodeUpdateAddressIfNeeded(sender, link, hdr);
                managers.nodes.clusterDelNode(link.node);
                return false;
            }

            managers.nodes.clusterRenameNode(link.node, hdr.sender);
            if (managers.configuration.isVerbose())
                logger.info("Handshake with node " + link.node.name + " completed.");
            link.node.flags &= ~CLUSTER_NODE_HANDSHAKE;
            link.node.flags |= hdr.flags & (CLUSTER_NODE_MASTER | CLUSTER_NODE_SLAVE);
        } else if (link.node != null && !link.node.name.equals(hdr.sender)) {
            logger.debug("PONG contains mismatching sender ID. About node " + link.node.name + " added " + (System.currentTimeMillis() - link.node.ctime) + " ms ago, having flags " + link.node.flags);
            link.node.flags |= CLUSTER_NODE_NOADDR;
            link.node.ip = null;
            link.node.port = 0;
            link.node.cport = 0;
            managers.connections.freeClusterLink(link);
            return false;
        }

        if (sender == null) return true;

        if (!States.nodeInHandshake(sender)) nodeUpdateAddressIfNeeded(sender, link, hdr);

        if (hdr.slaveof == null) {
            managers.nodes.clusterSetNodeAsMaster(sender);
        } else {
            ClusterNode master = managers.nodes.clusterLookupNode(hdr.slaveof);

            if (States.nodeIsMaster(sender)) {
                managers.slots.clusterDelNodeSlots(sender);
                sender.flags &= ~(CLUSTER_NODE_MASTER | CLUSTER_NODE_MIGRATE_TO);
                sender.flags |= CLUSTER_NODE_SLAVE;
            }

            if (master != null && (sender.slaveof == null || !sender.slaveof.equals(master))) {
                if (sender.slaveof != null) managers.nodes.clusterNodeRemoveSlave(sender.slaveof, sender);
                managers.nodes.clusterNodeAddSlave(master, sender);
                sender.slaveof = master;
            }
        }

        boolean dirtySlots = false;

        ClusterNode senderMaster = States.nodeIsMaster(sender) ? sender : sender.slaveof;
        if (senderMaster != null) {
            dirtySlots = !Arrays.equals(senderMaster.slots, hdr.myslots);
        }

        if (States.nodeIsMaster(sender) && dirtySlots) {
            clusterUpdateSlotsConfigWith(sender, hdr.configEpoch, hdr.myslots);
        }

        if (dirtySlots) {
            for (int i = 0; i < CLUSTER_SLOTS; i++) {
                if (!bitmapTestBit(hdr.myslots, i)) continue;
                if (server.cluster.slots[i] == null || server.cluster.slots[i].equals(sender)) continue;
                if (server.cluster.slots[i].configEpoch > hdr.configEpoch) {
                    if (managers.configuration.isVerbose())
                        logger.info("Node " + sender.name + " has old slots configuration, sending an UPDATE message about " + server.cluster.slots[i].name);
                    managers.messages.clusterSendUpdate(sender.link, server.cluster.slots[i]);
                    break;
                }
            }
        }

        if (States.nodeIsMaster(server.myself) && States.nodeIsMaster(sender) && hdr.configEpoch == server.myself.configEpoch) {
            clusterHandleConfigEpochCollision(sender);
        }

        clusterProcessGossipSection(hdr, link);
        return true;
    }
}

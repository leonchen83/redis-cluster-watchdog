package com.moilioncircle.redis.cluster.watchdog.message.handler;

import com.moilioncircle.redis.cluster.watchdog.ClusterConstants;
import com.moilioncircle.redis.cluster.watchdog.manager.ClusterManagers;
import com.moilioncircle.redis.cluster.watchdog.manager.ClusterSlotManger;
import com.moilioncircle.redis.cluster.watchdog.message.ClusterMessage;
import com.moilioncircle.redis.cluster.watchdog.message.ClusterMessageDataGossip;
import com.moilioncircle.redis.cluster.watchdog.state.ClusterLink;
import com.moilioncircle.redis.cluster.watchdog.state.ClusterNode;
import com.moilioncircle.redis.cluster.watchdog.state.ServerState;
import com.moilioncircle.redis.cluster.watchdog.state.States;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.List;

/**
 * Created by Baoyi Chen on 2017/7/13.
 */
public abstract class AbstractClusterMessageHandler implements ClusterMessageHandler {

    protected static final Log logger = LogFactory.getLog(AbstractClusterMessageHandler.class);

    protected ServerState server;
    protected ClusterManagers managers;

    public AbstractClusterMessageHandler(ClusterManagers managers) {
        this.managers = managers;
        this.server = managers.server;
    }

    @Override
    public boolean handle(ClusterLink link, ClusterMessage hdr) {
        int type = hdr.type;

        if (type < ClusterConstants.CLUSTERMSG_TYPE_COUNT) {
            server.cluster.statsBusMessagesReceived[type]++;
        }

        if (hdr.ver != ClusterConstants.CLUSTER_PROTO_VER) return true;

        ClusterNode sender = managers.nodes.clusterLookupNode(hdr.sender);
        if (sender != null && !States.nodeInHandshake(sender)) {
            if (hdr.currentEpoch > server.cluster.currentEpoch) {
                server.cluster.currentEpoch = hdr.currentEpoch;
            }
            if (hdr.configEpoch > sender.configEpoch) {
                sender.configEpoch = hdr.configEpoch;
            }
        }

        handle(sender, link, hdr);
        managers.states.clusterUpdateState();
        return true;
    }

    public abstract boolean handle(ClusterNode sender, ClusterLink link, ClusterMessage hdr);

    public void clusterUpdateSlotsConfigWith(ClusterNode sender, long senderConfigEpoch, byte[] slots) {
        ClusterNode newmaster = null;
        ClusterNode curmaster = States.nodeIsMaster(server.myself) ? server.myself : server.myself.slaveof;
        if (sender.equals(server.myself)) {
            logger.info("Discarding UPDATE message about myself.");
            return;
        }

        for (int i = 0; i < ClusterConstants.CLUSTER_SLOTS; i++) {
            if (!ClusterSlotManger.bitmapTestBit(slots, i)) continue;
            if (server.cluster.slots[i] != null && server.cluster.slots[i].equals(sender)) continue;
            if (server.cluster.slots[i] == null || server.cluster.slots[i].configEpoch < senderConfigEpoch) {
                if (server.cluster.slots[i] != null && server.cluster.slots[i].equals(curmaster))
                    newmaster = sender;
                managers.slots.clusterDelSlot(i);
                managers.slots.clusterAddSlot(sender, i);
            }
        }

        if (newmaster != null && curmaster.numslots == 0) {
            logger.info("Configuration change detected. Reconfiguring myself as a replica of " + sender.name);
            managers.nodes.clusterSetMyMaster(sender);
        }
    }

    public void clusterProcessGossipSection(ClusterMessage hdr, ClusterLink link) {
        List<ClusterMessageDataGossip> gs = hdr.data.gossip;
        ClusterNode sender = link.node != null ? link.node : managers.nodes.clusterLookupNode(hdr.sender);
        for (ClusterMessageDataGossip g : gs) {
            int flags = g.flags;
            String ci = managers.configs.representClusterNodeFlags(flags);
            logger.debug("GOSSIP " + g.nodename + " " + g.ip + ":" + g.port + "@" + g.cport + " " + ci);

            ClusterNode node = managers.nodes.clusterLookupNode(g.nodename);

            if (node == null) {
                if (sender != null && (flags & ClusterConstants.CLUSTER_NODE_NOADDR) == 0 && !managers.blacklists.clusterBlacklistExists(g.nodename)) {
                    managers.nodes.clusterStartHandshake(g.ip, g.port, g.cport);
                }
                continue;
            }

            if (sender != null && States.nodeIsMaster(sender) && !node.equals(server.myself)) {
                if ((flags & (ClusterConstants.CLUSTER_NODE_FAIL | ClusterConstants.CLUSTER_NODE_PFAIL)) != 0) {
                    if (managers.nodes.clusterNodeAddFailureReport(node, sender)) {
                        if (managers.configuration.isVerbose())
                            logger.info("Node " + sender.name + " reported node " + node.name + " as not reachable.");
                    }
                    markNodeAsFailingIfNeeded(node);
                } else if (managers.nodes.clusterNodeDelFailureReport(node, sender)) {
                    if (managers.configuration.isVerbose())
                        logger.info("Node " + sender.name + " reported node " + node.name + " is back online.");
                }
            }

            if ((flags & (ClusterConstants.CLUSTER_NODE_FAIL | ClusterConstants.CLUSTER_NODE_PFAIL)) == 0 && node.pingSent == 0 && managers.nodes.clusterNodeFailureReportsCount(node) == 0) {
                long pongtime = g.pongReceived;
                if (pongtime <= (System.currentTimeMillis() + 500) && pongtime > node.pongReceived) {
                    node.pongReceived = pongtime;
                }
            }

            if ((node.flags & (ClusterConstants.CLUSTER_NODE_FAIL | ClusterConstants.CLUSTER_NODE_PFAIL)) != 0 && (flags & ClusterConstants.CLUSTER_NODE_NOADDR) == 0 && (flags & (ClusterConstants.CLUSTER_NODE_FAIL | ClusterConstants.CLUSTER_NODE_PFAIL)) == 0 &&
                    (!node.ip.equalsIgnoreCase(g.ip) || node.port != g.port || node.cport != g.cport)) {
                if (node.link != null) managers.connections.freeClusterLink(node.link);
                node.ip = g.ip;
                node.port = g.port;
                node.cport = g.cport;
                node.flags &= ~ClusterConstants.CLUSTER_NODE_NOADDR;
            }
        }
    }

    public boolean nodeUpdateAddressIfNeeded(ClusterNode node, ClusterLink link, ClusterMessage hdr) {
        int port = hdr.port;
        int cport = hdr.cport;
        if (link.equals(node.link)) return false;

        String ip = link.fd.getRemoteAddress(hdr.myip);

        if (node.port == port && node.cport == cport && ip.equals(node.ip)) return false;

        node.ip = ip;
        node.port = port;
        node.cport = cport;

        if (node.link != null) managers.connections.freeClusterLink(node.link);
        logger.info("Address updated for node " + node.name + ", now " + node.ip + ":" + node.port);

        if (States.nodeIsSlave(server.myself) && server.myself.slaveof.equals(node)) {
            managers.replications.replicationSetMaster(node);
        }
        return true;
    }

    public void markNodeAsFailingIfNeeded(ClusterNode node) {
        int neededQuorum = server.cluster.size / 2 + 1;

        if (!States.nodePFailed(node) || States.nodeFailed(node)) return;

        int failures = managers.nodes.clusterNodeFailureReportsCount(node);

        if (States.nodeIsMaster(server.myself)) failures++;
        if (failures < neededQuorum) return;

        logger.info("Marking node " + node.name + " as failing (quorum reached).");

        node.flags &= ~ClusterConstants.CLUSTER_NODE_PFAIL;
        node.flags |= ClusterConstants.CLUSTER_NODE_FAIL;
        node.failTime = System.currentTimeMillis();

        if (States.nodeIsMaster(server.myself)) managers.messages.clusterSendFail(node.name);
    }

    public void clusterHandleConfigEpochCollision(ClusterNode sender) {
        if (sender.configEpoch != server.myself.configEpoch || States.nodeIsSlave(sender) || States.nodeIsSlave(server.myself))
            return;
        if (sender.name.compareTo(server.myself.name) <= 0) return;
        server.cluster.currentEpoch++;
        server.myself.configEpoch = server.cluster.currentEpoch;
        logger.warn("WARNING: configEpoch collision with node " + sender.name + ". configEpoch set to " + server.myself.configEpoch);
    }
}

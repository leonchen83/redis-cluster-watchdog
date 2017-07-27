package com.moilioncircle.redis.cluster.watchdog.message.handler;

import com.moilioncircle.redis.cluster.watchdog.manager.ClusterManagers;
import com.moilioncircle.redis.cluster.watchdog.message.ClusterMessage;
import com.moilioncircle.redis.cluster.watchdog.state.ClusterLink;
import com.moilioncircle.redis.cluster.watchdog.state.ClusterNode;

import static com.moilioncircle.redis.cluster.watchdog.ClusterConstants.CLUSTERMSG_FLAG0_FORCEACK;
import static com.moilioncircle.redis.cluster.watchdog.ClusterConstants.CLUSTER_SLOTS;
import static com.moilioncircle.redis.cluster.watchdog.manager.ClusterSlotManger.bitmapTestBit;
import static com.moilioncircle.redis.cluster.watchdog.state.NodeStates.*;

/**
 * @author Leon Chen
 * @since 1.0.0
 */
public class ClusterMessageFailoverAuthRequestHandler extends AbstractClusterMessageHandler {
    public ClusterMessageFailoverAuthRequestHandler(ClusterManagers managers) {
        super(managers);
    }

    @Override
    public boolean handle(ClusterNode sender, ClusterLink link, ClusterMessage hdr) {
        logger.debug("Failover auth request packet received: node:" + link.node == null ? "(nil)" : link.node.name);
        if (sender == null) return true;
        clusterSendFailoverAuthIfNeeded(sender, hdr);
        return true;
    }

    public void clusterSendFailoverAuthIfNeeded(ClusterNode node, ClusterMessage hdr) {

        boolean force = (hdr.messageFlags[0] & CLUSTERMSG_FLAG0_FORCEACK) != 0;

        if (nodeIsSlave(server.myself) || server.myself.assignedSlots == 0)
            return;

        if (hdr.currentEpoch < server.cluster.currentEpoch) {
            logger.warn("Failover auth denied to " + node.name + ": reqEpoch " + hdr.currentEpoch + " < curEpoch(" + server.cluster.currentEpoch + ")");
            return;
        }

        if (server.cluster.lastVoteEpoch == server.cluster.currentEpoch) {
            logger.warn("Failover auth denied to " + node.name + ": already voted for epoch " + server.cluster.currentEpoch);
            return;
        }

        if (nodeIsMaster(node)) {
            logger.warn("Failover auth denied to " + node.name + ": it is a master node");
            return;
        } else if (node.master == null) {
            logger.warn("Failover auth denied to " + node.name + ": I don't know its master");
            return;
        } else if (!nodeFailed(node.master) && !force) {
            logger.warn("Failover auth denied to " + node.name + ": its master is up");
            return;
        }

        long now = System.currentTimeMillis();
        if (now - node.master.votedTime < managers.configuration.getClusterNodeTimeout() * 2) {
            logger.warn("Failover auth denied to " + node.name + ": can't vote fail this master before " + (managers.configuration.getClusterNodeTimeout() * 2 - (now - node.master.votedTime)) + " milliseconds");
            return;
        }

        for (int i = 0; i < CLUSTER_SLOTS; i++) {
            if (!bitmapTestBit(hdr.slots, i)) continue;
            if (server.cluster.slots[i] == null) continue;
            if (server.cluster.slots[i].configEpoch <= hdr.configEpoch) continue;
            logger.warn("Failover auth denied to " + node.name + ": slot " + i + " epoch (" + server.cluster.slots[i].configEpoch + ") > reqEpoch (" + hdr.configEpoch + ")");
            return;
        }

        managers.messages.clusterSendFailoverAuth(node);
        node.master.votedTime = System.currentTimeMillis();
        server.cluster.lastVoteEpoch = server.cluster.currentEpoch;
        logger.warn("Failover auth granted to " + node.name + " for epoch " + server.cluster.currentEpoch);
    }
}

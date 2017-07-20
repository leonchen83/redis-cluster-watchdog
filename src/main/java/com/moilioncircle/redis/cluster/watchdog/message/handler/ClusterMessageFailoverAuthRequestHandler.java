package com.moilioncircle.redis.cluster.watchdog.message.handler;

import com.moilioncircle.redis.cluster.watchdog.ClusterConstants;
import com.moilioncircle.redis.cluster.watchdog.manager.ClusterManagers;
import com.moilioncircle.redis.cluster.watchdog.manager.ClusterSlotManger;
import com.moilioncircle.redis.cluster.watchdog.message.ClusterMessage;
import com.moilioncircle.redis.cluster.watchdog.state.ClusterLink;
import com.moilioncircle.redis.cluster.watchdog.state.ClusterNode;
import com.moilioncircle.redis.cluster.watchdog.state.States;

/**
 * Created by Baoyi Chen on 2017/7/13.
 */
public class ClusterMessageFailoverAuthRequestHandler extends AbstractClusterMessageHandler {
    public ClusterMessageFailoverAuthRequestHandler(ClusterManagers gossip) {
        super(gossip);
    }

    @Override
    public boolean handle(ClusterNode sender, ClusterLink link, ClusterMessage hdr) {
        logger.debug("Failover auth request packet received: " + Thread.currentThread() + ",node:" + link.node + ",sender:" + sender + ",message:" + hdr);
        if (sender == null) return true;
        clusterSendFailoverAuthIfNeeded(sender, hdr);
        return true;
    }

    public void clusterSendFailoverAuthIfNeeded(ClusterNode node, ClusterMessage request) {
        ClusterNode master = node.slaveof;
        long requestCurrentEpoch = request.currentEpoch;
        long requestConfigEpoch = request.configEpoch;
        byte[] claimedSlots = request.myslots;
        boolean forceAck = (request.mflags[0] & ClusterConstants.CLUSTERMSG_FLAG0_FORCEACK) != 0;

        if (States.nodeIsSlave(server.myself) || server.myself.numslots == 0) return;

        if (requestCurrentEpoch < server.cluster.currentEpoch) {
            logger.warn("Failover auth denied to " + node.name + ": reqEpoch " + requestCurrentEpoch + " < curEpoch(" + server.cluster.currentEpoch + ")");
            return;
        }

        if (server.cluster.lastVoteEpoch == server.cluster.currentEpoch) {
            logger.warn("Failover auth denied to " + node.name + ": already voted for epoch " + server.cluster.currentEpoch);
            return;
        }

        if (States.nodeIsMaster(node) || master == null || (!States.nodeFailed(master) && !forceAck)) {
            if (States.nodeIsMaster(node)) {
                logger.warn("Failover auth denied to " + node.name + ": it is a master node");
            } else if (master == null) {
                logger.warn("Failover auth denied to " + node.name + ": I don't know its master");
            } else if (!States.nodeFailed(master)) {
                logger.warn("Failover auth denied to " + node.name + ": its master is up");
            }
            return;
        }

        if (System.currentTimeMillis() - node.slaveof.votedTime < managers.configuration.getClusterNodeTimeout() * 2) {
            logger.warn("Failover auth denied to " + node.name + ": can't vote about this master before " + (managers.configuration.getClusterNodeTimeout() * 2 - (System.currentTimeMillis() - node.slaveof.votedTime)) + " milliseconds");
            return;
        }

        for (int i = 0; i < ClusterConstants.CLUSTER_SLOTS; i++) {
            if (!ClusterSlotManger.bitmapTestBit(claimedSlots, i)) continue;
            if (server.cluster.slots[i] == null || server.cluster.slots[i].configEpoch <= requestConfigEpoch)
                continue;

            logger.warn("Failover auth denied to " + node.name + ": slot %d epoch (" + server.cluster.slots[i].configEpoch + ") > reqEpoch (" + requestConfigEpoch + ")");
            return;
        }

        managers.messages.clusterSendFailoverAuth(node);
        server.cluster.lastVoteEpoch = server.cluster.currentEpoch;
        node.slaveof.votedTime = System.currentTimeMillis();
        logger.warn("Failover auth granted to " + node.name + " for epoch " + server.cluster.currentEpoch);
    }
}

package com.moilioncircle.redis.cluster.watchdog.message.handler;

import com.moilioncircle.redis.cluster.watchdog.manager.ClusterManagers;
import com.moilioncircle.redis.cluster.watchdog.message.ClusterMessage;
import com.moilioncircle.redis.cluster.watchdog.state.ClusterLink;
import com.moilioncircle.redis.cluster.watchdog.state.ClusterNode;

import java.util.concurrent.ThreadLocalRandom;

import static com.moilioncircle.redis.cluster.watchdog.ClusterConstants.*;
import static com.moilioncircle.redis.cluster.watchdog.ConfigInfo.valueOf;
import static com.moilioncircle.redis.cluster.watchdog.manager.ClusterSlotManger.bitmapTestBit;
import static com.moilioncircle.redis.cluster.watchdog.state.States.nodeFailed;
import static com.moilioncircle.redis.cluster.watchdog.state.States.nodeIsMaster;

/**
 * @author Leon Chen
 * @since 1.0.0
 */
public class ClusterMessageFailoverAuthAckHandler extends AbstractClusterMessageHandler {
    public ClusterMessageFailoverAuthAckHandler(ClusterManagers managers) {
        super(managers);
    }

    @Override
    public boolean handle(ClusterNode sender, ClusterLink link, ClusterMessage hdr) {
        if (logger.isDebugEnabled()) {
            logger.debug("Failover auth ack packet received: node:" + link.node + ",name:" + sender + ",message:" + hdr);
        }

        if (sender == null) return true;
        if (nodeIsMaster(sender) && sender.assignedSlots > 0 && hdr.currentEpoch >= server.cluster.failoverAuthEpoch) {
            server.cluster.failoverAuthEpoch++;
            clusterHandleSlaveFailover();
        }
        return true;
    }

    public void clusterHandleSlaveFailover() {
        long authAge = System.currentTimeMillis() - server.cluster.failoverAuthTime;
        int quorum = (server.cluster.size / 2) + 1;
        long authTimeout = Math.max(managers.configuration.getClusterNodeTimeout() * 2, 2000);
        long authRetryTime = authTimeout * 2;

        if (nodeIsMaster(server.myself) || server.myself.master == null ||
                !nodeFailed(server.myself.master) || server.myself.master.assignedSlots == 0) {
            return;
        }

        long now = System.currentTimeMillis();
        if (authAge > authRetryTime) {
            server.cluster.failoverAuthTime = now + 500 + ThreadLocalRandom.current().nextInt(500);
            server.cluster.failoverAuthCount = 0;
            server.cluster.failoverAuthSent = false;
            server.cluster.failoverAuthRank = managers.nodes.clusterGetSlaveRank();
            server.cluster.failoverAuthTime += server.cluster.failoverAuthRank * 1000;
            logger.info("Start of election delayed for " + (server.cluster.failoverAuthTime - now) + " milliseconds (rank #" + server.cluster.failoverAuthRank + ", offset " + managers.replications.replicationGetSlaveOffset() + ").");
            managers.messages.clusterBroadcastPong(CLUSTER_BROADCAST_LOCAL_SLAVES);
            return;
        }

        if (!server.cluster.failoverAuthSent) {
            int rank = managers.nodes.clusterGetSlaveRank();
            if (rank > server.cluster.failoverAuthRank) {
                long delay = (rank - server.cluster.failoverAuthRank) * 1000;
                server.cluster.failoverAuthTime += delay;
                server.cluster.failoverAuthRank = rank;
                logger.info("Slave rank updated to #" + rank + ", added " + delay + " milliseconds of delay.");
            }
        }

        if (System.currentTimeMillis() < server.cluster.failoverAuthTime) {
            return;
        }

        if (authAge > authTimeout) {
            return;
        }

        if (!server.cluster.failoverAuthSent) {
            server.cluster.currentEpoch++;
            server.cluster.failoverAuthEpoch = server.cluster.currentEpoch;
            logger.info("Starting a failover election for epoch " + server.cluster.currentEpoch + ".");
            managers.messages.clusterRequestFailoverAuth();
            server.cluster.failoverAuthSent = true;
            return;
        }

        if (server.cluster.failoverAuthCount >= quorum) {
            logger.info("Failover election won: I'm the new master.");

            if (server.myself.configEpoch < server.cluster.failoverAuthEpoch) {
                server.myself.configEpoch = server.cluster.failoverAuthEpoch;
                logger.info("configEpoch set to " + server.myself.configEpoch + " after successful failover");
            }
            clusterFailoverReplaceMyMaster();
        }
    }

    public void clusterFailoverReplaceMyMaster() {
        ClusterNode previous = server.myself.master;
        if (nodeIsMaster(server.myself) || previous == null) return;
        managers.nodes.clusterSetNodeAsMaster(server.myself);
        managers.replications.replicationUnsetMaster();

        for (int i = 0; i < CLUSTER_SLOTS; i++) {
            if (!bitmapTestBit(previous.slots, i)) continue;
            managers.slots.clusterDelSlot(i);
            managers.slots.clusterAddSlot(server.myself, i);
        }

        managers.states.clusterUpdateState();
        managers.configs.clusterSaveConfig(valueOf(server.cluster));
        managers.messages.clusterBroadcastPong(CLUSTER_BROADCAST_ALL);
    }
}

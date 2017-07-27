package com.moilioncircle.redis.cluster.watchdog.message.handler;

import com.moilioncircle.redis.cluster.watchdog.manager.ClusterManagers;
import com.moilioncircle.redis.cluster.watchdog.message.ClusterMessage;
import com.moilioncircle.redis.cluster.watchdog.state.ClusterLink;
import com.moilioncircle.redis.cluster.watchdog.state.ClusterNode;

import static com.moilioncircle.redis.cluster.watchdog.state.NodeStates.nodeIsMaster;

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
        logger.debug("Failover auth ack packet received: node:" + (link.node == null ? "(nil)" : link.node.name));

        if (sender == null) return true;
        if (nodeIsMaster(sender) && sender.assignedSlots > 0 && hdr.currentEpoch >= server.cluster.failoverAuthEpoch) {
            server.cluster.failoverAuthCount++;
            managers.failovers.clusterHandleSlaveFailover();
        }
        return true;
    }
}

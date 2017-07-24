package com.moilioncircle.redis.cluster.watchdog.message.handler;

import com.moilioncircle.redis.cluster.watchdog.manager.ClusterManagers;
import com.moilioncircle.redis.cluster.watchdog.message.ClusterMessage;
import com.moilioncircle.redis.cluster.watchdog.state.ClusterLink;
import com.moilioncircle.redis.cluster.watchdog.state.ClusterNode;

import static com.moilioncircle.redis.cluster.watchdog.state.NodeStates.nodeIsSlave;

/**
 * @author Leon Chen
 * @since 1.0.0
 */
public class ClusterMessageUpdateHandler extends AbstractClusterMessageHandler {
    public ClusterMessageUpdateHandler(ClusterManagers managers) {
        super(managers);
    }

    @Override
    public boolean handle(ClusterNode sender, ClusterLink link, ClusterMessage hdr) {
        if (logger.isDebugEnabled()) {
            logger.debug("Update packet received: node:" + link.node + ",name:" + sender + ",message:" + hdr);
        }
        if (sender == null) return true;
        long configEpoch = hdr.data.config.configEpoch;
        ClusterNode node = managers.nodes.clusterLookupNode(hdr.data.config.name);
        if (node == null || node.configEpoch >= configEpoch) return true;
        if (nodeIsSlave(node)) managers.nodes.clusterSetNodeAsMaster(node);
        node.configEpoch = configEpoch;
        clusterUpdateSlotsConfigWith(node, configEpoch, hdr.data.config.slots);
        return true;
    }
}

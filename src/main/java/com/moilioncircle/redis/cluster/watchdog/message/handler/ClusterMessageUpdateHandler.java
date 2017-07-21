package com.moilioncircle.redis.cluster.watchdog.message.handler;

import com.moilioncircle.redis.cluster.watchdog.manager.ClusterManagers;
import com.moilioncircle.redis.cluster.watchdog.message.ClusterMessage;
import com.moilioncircle.redis.cluster.watchdog.state.ClusterLink;
import com.moilioncircle.redis.cluster.watchdog.state.ClusterNode;

import static com.moilioncircle.redis.cluster.watchdog.state.States.nodeIsSlave;

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
        long configEpoch = hdr.data.config.configEpoch;
        if (sender == null) return true;
        ClusterNode n = managers.nodes.clusterLookupNode(hdr.data.config.name);
        if (n == null) return true;
        if (n.configEpoch >= configEpoch) return true;

        if (nodeIsSlave(n)) managers.nodes.clusterSetNodeAsMaster(n);

        n.configEpoch = configEpoch;

        clusterUpdateSlotsConfigWith(n, configEpoch, hdr.data.config.slots);
        return true;
    }
}

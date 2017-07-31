package com.moilioncircle.redis.cluster.watchdog.message.handler;

import com.moilioncircle.redis.cluster.watchdog.manager.ClusterManagers;
import com.moilioncircle.redis.cluster.watchdog.message.ClusterMessage;
import com.moilioncircle.redis.cluster.watchdog.state.ClusterLink;
import com.moilioncircle.redis.cluster.watchdog.state.ClusterNode;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import static com.moilioncircle.redis.cluster.watchdog.state.NodeStates.nodeIsSlave;

/**
 * @author Leon Chen
 * @since 1.0.0
 */
public class ClusterMessageUpdateHandler extends AbstractClusterMessageHandler {

    private static final Log logger = LogFactory.getLog(ClusterMessageUpdateHandler.class);

    public ClusterMessageUpdateHandler(ClusterManagers managers) {
        super(managers);
    }

    @Override
    public boolean handle(ClusterNode sender, ClusterLink link, ClusterMessage hdr) {
        logger.debug("Update packet received: node:" + (link.node == null ? "(nil)" : link.node.name));
        if (sender == null) return true;
        String name = hdr.data.config.name;
        long epoch = hdr.data.config.configEpoch;
        ClusterNode node = managers.nodes.clusterLookupNode(name);
        if (node == null || node.configEpoch >= epoch) return true;
        if (nodeIsSlave(node)) managers.nodes.clusterSetNodeAsMaster(node);
        clusterUpdateSlotsConfigWith(node, node.configEpoch = epoch, hdr.data.config.slots); return true;
    }
}

package com.moilioncircle.redis.cluster.watchdog.message.handler;

import com.moilioncircle.redis.cluster.watchdog.manager.ClusterManagers;
import com.moilioncircle.redis.cluster.watchdog.message.ClusterMessage;
import com.moilioncircle.redis.cluster.watchdog.state.ClusterLink;
import com.moilioncircle.redis.cluster.watchdog.state.ClusterNode;

/**
 * @author Leon Chen
 * @since 1.0.0
 */
public class ClusterMessageMFStartHandler extends AbstractClusterMessageHandler {
    public ClusterMessageMFStartHandler(ClusterManagers managers) {
        super(managers);
    }

    @Override
    public boolean handle(ClusterNode sender, ClusterLink link, ClusterMessage hdr) {
        logger.debug("MFStart packet received: node:" + link.node == null ? "(nil)" : link.node.name);
        return true;
    }
}

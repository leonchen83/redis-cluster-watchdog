package com.moilioncircle.redis.cluster.watchdog.message.handler;

import com.moilioncircle.redis.cluster.watchdog.manager.ClusterManagers;
import com.moilioncircle.redis.cluster.watchdog.message.ClusterMessage;
import com.moilioncircle.redis.cluster.watchdog.state.ClusterLink;
import com.moilioncircle.redis.cluster.watchdog.state.ClusterNode;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * @author Leon Chen
 * @since 1.0.0
 */
public class ClusterMessagePublishHandler extends AbstractClusterMessageHandler {

    private static final Log logger = LogFactory.getLog(ClusterMessagePublishHandler.class);

    public ClusterMessagePublishHandler(ClusterManagers managers) {
        super(managers);
    }

    @Override
    public boolean handle(ClusterNode sender, ClusterLink link, ClusterMessage hdr) {
        logger.debug("Publish packet received: node:" + (link.node == null ? "(nil)" : link.node.name));
        return true;
    }
}

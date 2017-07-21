package com.moilioncircle.redis.cluster.watchdog.message.handler;

import com.moilioncircle.redis.cluster.watchdog.manager.ClusterManagers;
import com.moilioncircle.redis.cluster.watchdog.message.ClusterMessage;
import com.moilioncircle.redis.cluster.watchdog.state.ClusterLink;
import com.moilioncircle.redis.cluster.watchdog.state.ClusterNode;

/**
 * @author Leon Chen
 * @since 1.0.0
 */
public class ClusterMessagePublishHandler extends AbstractClusterMessageHandler {
    public ClusterMessagePublishHandler(ClusterManagers managers) {
        super(managers);
    }

    @Override
    public boolean handle(ClusterNode sender, ClusterLink link, ClusterMessage hdr) {
        if (logger.isDebugEnabled()) {
            logger.debug("Publish packet received: node:" + link.node + ",name:" + sender + ",message:" + hdr);
        }
        return true;
    }
}

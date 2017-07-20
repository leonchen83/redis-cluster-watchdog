package com.moilioncircle.redis.cluster.watchdog.message.handler;

import com.moilioncircle.redis.cluster.watchdog.manager.ClusterManagers;
import com.moilioncircle.redis.cluster.watchdog.message.ClusterMessage;
import com.moilioncircle.redis.cluster.watchdog.state.ClusterLink;
import com.moilioncircle.redis.cluster.watchdog.state.ClusterNode;

/**
 * Created by Baoyi Chen on 2017/7/13.
 */
public class ClusterMessageFailoverAuthAckHandler extends AbstractClusterMessageHandler {
    public ClusterMessageFailoverAuthAckHandler(ClusterManagers gossip) {
        super(gossip);
    }

    @Override
    public boolean handle(ClusterNode sender, ClusterLink link, ClusterMessage hdr) {
        if (logger.isDebugEnabled()) {
            logger.debug("Failover auth ack packet received: node:" + link.node + ",sender:" + sender + ",message:" + hdr);
        }
        return true;
    }
}

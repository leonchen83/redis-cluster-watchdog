package com.moilioncircle.replicator.cluster.message.handler;

import com.moilioncircle.replicator.cluster.manager.ClusterManagers;
import com.moilioncircle.replicator.cluster.message.ClusterMessage;
import com.moilioncircle.replicator.cluster.state.ClusterLink;
import com.moilioncircle.replicator.cluster.state.ClusterNode;

/**
 * Created by Baoyi Chen on 2017/7/13.
 */
public class ClusterMessagePublishHandler extends AbstractClusterMessageHandler {
    public ClusterMessagePublishHandler(ClusterManagers gossip) {
        super(gossip);
    }

    @Override
    public boolean handle(ClusterNode sender, ClusterLink link, ClusterMessage hdr) {
        logger.debug("Publish packet received: " + Thread.currentThread() + ",node:" + link.node + ",sender:" + sender + ",message:" + hdr);
        return true;
    }
}

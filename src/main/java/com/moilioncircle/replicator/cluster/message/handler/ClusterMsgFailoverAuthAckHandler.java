package com.moilioncircle.replicator.cluster.message.handler;

import com.moilioncircle.replicator.cluster.ClusterLink;
import com.moilioncircle.replicator.cluster.ClusterNode;
import com.moilioncircle.replicator.cluster.gossip.ThinGossip;
import com.moilioncircle.replicator.cluster.message.ClusterMsg;

/**
 * Created by Baoyi Chen on 2017/7/13.
 */
public class ClusterMsgFailoverAuthAckHandler extends AbstractClusterMsgHandler {
    public ClusterMsgFailoverAuthAckHandler(ThinGossip gossip) {
        super(gossip);
    }

    @Override
    public boolean handle(ClusterNode sender, ClusterLink link, ClusterMsg hdr) {
        logger.debug("Failover auth ack packet received: " + Thread.currentThread() + ",node:" + link.node + ",sender:" + sender + ",message:" + hdr);
        return true;
    }
}

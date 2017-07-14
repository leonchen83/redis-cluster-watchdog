package com.moilioncircle.replicator.cluster.message.handler;

import com.moilioncircle.replicator.cluster.ClusterLink;
import com.moilioncircle.replicator.cluster.ClusterNode;
import com.moilioncircle.replicator.cluster.gossip.ThinGossip;
import com.moilioncircle.replicator.cluster.message.ClusterMsg;

import static com.moilioncircle.replicator.cluster.ClusterConstants.*;

/**
 * Created by Baoyi Chen on 2017/7/13.
 */
public class ClusterMsgFailHandler extends AbstractClusterMsgHandler {

    public ClusterMsgFailHandler(ThinGossip gossip) {
        super(gossip);
    }

    @Override
    public boolean handle(ClusterNode sender, ClusterLink link, ClusterMsg hdr) {
        logger.debug("Fail packet received: " + link.node);

        if (sender == null) {
            logger.info("Ignoring FAIL message from unknown node " + hdr.sender + " about " + hdr.data.about.nodename);
            return true;
        }
        ClusterNode failing = gossip.nodeManager.clusterLookupNode(hdr.data.about.nodename);
        if (failing != null && (failing.flags & (CLUSTER_NODE_FAIL | CLUSTER_NODE_MYSELF)) == 0) {
            logger.info("FAIL message received from " + hdr.sender + " about " + hdr.data.about.nodename);
            failing.flags |= CLUSTER_NODE_FAIL;
            failing.failTime = System.currentTimeMillis();
            failing.flags &= ~CLUSTER_NODE_PFAIL;
            gossip.clusterDoBeforeSleep(CLUSTER_TODO_SAVE_CONFIG | CLUSTER_TODO_UPDATE_STATE);
        }
        return true;
    }
}

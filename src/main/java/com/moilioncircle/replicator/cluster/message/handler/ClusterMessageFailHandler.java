package com.moilioncircle.replicator.cluster.message.handler;

import com.moilioncircle.replicator.cluster.manager.ClusterManagers;
import com.moilioncircle.replicator.cluster.message.ClusterMessage;
import com.moilioncircle.replicator.cluster.state.ClusterLink;
import com.moilioncircle.replicator.cluster.state.ClusterNode;

import static com.moilioncircle.replicator.cluster.ClusterConstants.*;

/**
 * Created by Baoyi Chen on 2017/7/13.
 */
public class ClusterMessageFailHandler extends AbstractClusterMessageHandler {

    public ClusterMessageFailHandler(ClusterManagers gossip) {
        super(gossip);
    }

    @Override
    public boolean handle(ClusterNode sender, ClusterLink link, ClusterMessage hdr) {
        logger.debug("Fail packet received: " + Thread.currentThread() + ",node:" + link.node + ",sender:" + sender + ",message:" + hdr);

        if (sender == null) {
            logger.info("Ignoring FAIL message from unknown node " + hdr.sender + " about " + hdr.data.about.nodename);
            return true;
        }
        ClusterNode failing = managers.nodes.clusterLookupNode(hdr.data.about.nodename);
        if (failing != null && (failing.flags & (CLUSTER_NODE_FAIL | CLUSTER_NODE_MYSELF)) == 0) {
            logger.info("FAIL message received from " + hdr.sender + " about " + hdr.data.about.nodename);
            failing.flags |= CLUSTER_NODE_FAIL;
            failing.failTime = System.currentTimeMillis();
            failing.flags &= ~CLUSTER_NODE_PFAIL;
        }
        return true;
    }
}

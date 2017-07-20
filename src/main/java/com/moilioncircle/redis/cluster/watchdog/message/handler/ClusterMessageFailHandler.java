package com.moilioncircle.redis.cluster.watchdog.message.handler;

import com.moilioncircle.redis.cluster.watchdog.manager.ClusterManagers;
import com.moilioncircle.redis.cluster.watchdog.message.ClusterMessage;
import com.moilioncircle.redis.cluster.watchdog.state.ClusterLink;
import com.moilioncircle.redis.cluster.watchdog.state.ClusterNode;

import static com.moilioncircle.redis.cluster.watchdog.ClusterConstants.*;

/**
 * Created by Baoyi Chen on 2017/7/13.
 */
public class ClusterMessageFailHandler extends AbstractClusterMessageHandler {

    public ClusterMessageFailHandler(ClusterManagers gossip) {
        super(gossip);
    }

    @Override
    public boolean handle(ClusterNode sender, ClusterLink link, ClusterMessage hdr) {
        if (logger.isDebugEnabled()) {
            logger.debug("Fail packet received: node:" + link.node + ",sender:" + sender + ",message:" + hdr);
        }

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

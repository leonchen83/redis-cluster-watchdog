package com.moilioncircle.redis.cluster.watchdog.message.handler;

import com.moilioncircle.redis.cluster.watchdog.manager.ClusterManagers;
import com.moilioncircle.redis.cluster.watchdog.message.ClusterMessage;
import com.moilioncircle.redis.cluster.watchdog.state.ClusterLink;
import com.moilioncircle.redis.cluster.watchdog.state.ClusterNode;

import static com.moilioncircle.redis.cluster.watchdog.ClusterConstants.*;

/**
 * @author Leon Chen
 * @since 1.0.0
 */
public class ClusterMessageFailHandler extends AbstractClusterMessageHandler {

    public ClusterMessageFailHandler(ClusterManagers managers) {
        super(managers);
    }

    @Override
    public boolean handle(ClusterNode sender, ClusterLink link, ClusterMessage hdr) {
        if (logger.isDebugEnabled()) {
            logger.debug("Fail packet received: node:" + link.node + ",name:" + sender + ",message:" + hdr);
        }

        if (sender == null) {
            logger.info("Ignoring FAIL message from unknown node " + hdr.name + " fail " + hdr.data.fail.name);
            return true;
        }
        ClusterNode failing = managers.nodes.clusterLookupNode(hdr.data.fail.name);
        if (failing != null && (failing.flags & (CLUSTER_NODE_FAIL | CLUSTER_NODE_MYSELF)) == 0) {
            logger.info("FAIL message received from " + hdr.name + " fail " + hdr.data.fail.name);
            failing.flags |= CLUSTER_NODE_FAIL;
            failing.failTime = System.currentTimeMillis();
            failing.flags &= ~CLUSTER_NODE_PFAIL;
        }
        return true;
    }
}

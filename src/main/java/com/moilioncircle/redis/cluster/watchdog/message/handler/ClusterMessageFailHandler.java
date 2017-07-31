package com.moilioncircle.redis.cluster.watchdog.message.handler;

import com.moilioncircle.redis.cluster.watchdog.manager.ClusterManagers;
import com.moilioncircle.redis.cluster.watchdog.message.ClusterMessage;
import com.moilioncircle.redis.cluster.watchdog.state.ClusterLink;
import com.moilioncircle.redis.cluster.watchdog.state.ClusterNode;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import static com.moilioncircle.redis.cluster.watchdog.ClusterConstants.*;
import static com.moilioncircle.redis.cluster.watchdog.ClusterNodeInfo.valueOf;

/**
 * @author Leon Chen
 * @since 1.0.0
 */
public class ClusterMessageFailHandler extends AbstractClusterMessageHandler {

    private static final Log logger = LogFactory.getLog(ClusterMessageFailHandler.class);

    public ClusterMessageFailHandler(ClusterManagers managers) {
        super(managers);
    }

    @Override
    public boolean handle(ClusterNode sender, ClusterLink link, ClusterMessage hdr) {
        logger.debug("Fail packet received: node:" + (link.node == null ? "(nil)" : link.node.name));

        if (sender == null) {
            logger.info("Ignoring FAIL message from unknown node " + hdr.name + " fail " + hdr.data.fail.name);
            return true;
        }

        ClusterNode failing = managers.nodes.clusterLookupNode(hdr.data.fail.name);
        if (failing != null && (failing.flags & (CLUSTER_NODE_FAIL | CLUSTER_NODE_MYSELF)) == 0) {
            logger.info("FAIL message received from " + hdr.name + " fail " + hdr.data.fail.name);
            failing.flags |= CLUSTER_NODE_FAIL; failing.failTime = System.currentTimeMillis();
            failing.flags &= ~CLUSTER_NODE_PFAIL; managers.notifyNodeFailed(valueOf(failing, server.myself));
        }
        return true;
    }
}

package com.moilioncircle.replicator.cluster.message.handler;

import com.moilioncircle.replicator.cluster.manager.ClusterManagers;
import com.moilioncircle.replicator.cluster.message.ClusterMessage;
import com.moilioncircle.replicator.cluster.state.ClusterLink;
import com.moilioncircle.replicator.cluster.state.ClusterNode;

import static com.moilioncircle.replicator.cluster.state.States.nodeIsSlave;

/**
 * Created by Baoyi Chen on 2017/7/13.
 */
public class ClusterMessageUpdateHandler extends AbstractClusterMessageHandler {
    public ClusterMessageUpdateHandler(ClusterManagers gossip) {
        super(gossip);
    }

    @Override
    public boolean handle(ClusterNode sender, ClusterLink link, ClusterMessage hdr) {
        logger.debug("Update packet received: " + Thread.currentThread() + ",node:" + link.node + ",sender:" + sender + ",message:" + hdr);
        long reportedConfigEpoch = hdr.data.nodecfg.configEpoch;
        if (sender == null) return true;
        ClusterNode n = managers.nodes.clusterLookupNode(hdr.data.nodecfg.nodename);
        if (n == null) return true;
        if (n.configEpoch >= reportedConfigEpoch) return true;

        if (nodeIsSlave(n)) managers.nodes.clusterSetNodeAsMaster(n);

        n.configEpoch = reportedConfigEpoch;

        clusterUpdateSlotsConfigWith(n, reportedConfigEpoch, hdr.data.nodecfg.slots);
        return true;
    }
}

package com.moilioncircle.replicator.cluster.message.handler;

import com.moilioncircle.replicator.cluster.ClusterLink;
import com.moilioncircle.replicator.cluster.ClusterNode;
import com.moilioncircle.replicator.cluster.gossip.ThinGossip1;
import com.moilioncircle.replicator.cluster.message.ClusterMsg;

/**
 * Created by Baoyi Chen on 2017/7/13.
 */
public class ClusterMsgFailoverAuthAckHandler extends AbstractClusterMsgHandler {
    public ClusterMsgFailoverAuthAckHandler(ThinGossip1 gossip) {
        super(gossip);
    }

    @Override
    public boolean handle(ClusterNode sender, ClusterLink link, ClusterMsg hdr) {
        return true;
    }
}

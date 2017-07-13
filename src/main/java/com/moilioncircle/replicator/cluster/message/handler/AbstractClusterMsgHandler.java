package com.moilioncircle.replicator.cluster.message.handler;

import com.moilioncircle.replicator.cluster.ClusterNode;
import com.moilioncircle.replicator.cluster.gossip.Server;
import com.moilioncircle.replicator.cluster.gossip.ThinGossip1;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Created by Baoyi Chen on 2017/7/13.
 */
public abstract class AbstractClusterMsgHandler implements ClusterMsgHandler {

    protected static final Log logger = LogFactory.getLog(AbstractClusterMsgHandler.class);

    protected Server server;
    protected ThinGossip1 gossip;
    protected ClusterNode myself;

    public AbstractClusterMsgHandler(ThinGossip1 gossip) {
        this.gossip = gossip;
        this.server = gossip.server;
        this.myself = gossip.myself;
    }
}

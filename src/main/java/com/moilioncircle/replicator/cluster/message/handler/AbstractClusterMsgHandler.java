package com.moilioncircle.replicator.cluster.message.handler;

import com.moilioncircle.replicator.cluster.gossip.Server;
import com.moilioncircle.replicator.cluster.gossip.ThinGossip;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Created by Baoyi Chen on 2017/7/13.
 */
public abstract class AbstractClusterMsgHandler implements ClusterMsgHandler {

    protected static final Log logger = LogFactory.getLog(AbstractClusterMsgHandler.class);

    protected Server server;
    protected ThinGossip gossip;

    public AbstractClusterMsgHandler(ThinGossip gossip) {
        this.gossip = gossip;
        this.server = gossip.server;
    }
}

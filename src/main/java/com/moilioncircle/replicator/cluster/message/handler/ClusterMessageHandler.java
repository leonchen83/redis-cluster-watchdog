package com.moilioncircle.replicator.cluster.message.handler;

import com.moilioncircle.replicator.cluster.message.ClusterMessage;
import com.moilioncircle.replicator.cluster.state.ClusterLink;

/**
 * Created by Baoyi Chen on 2017/7/13.
 */
public interface ClusterMessageHandler {
    boolean handle(ClusterLink link, ClusterMessage hdr);
}

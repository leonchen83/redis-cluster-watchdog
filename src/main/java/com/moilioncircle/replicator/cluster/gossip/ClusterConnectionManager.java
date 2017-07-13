package com.moilioncircle.replicator.cluster.gossip;

import com.moilioncircle.replicator.cluster.ClusterLink;
import com.moilioncircle.replicator.cluster.ClusterNode;

/**
 * Created by Baoyi Chen on 2017/7/12.
 */
public class ClusterConnectionManager {
    public ClusterLink createClusterLink(ClusterNode node) {
        ClusterLink link = new ClusterLink();
        link.ctime = System.currentTimeMillis();
        link.node = node;
        return link;
    }

    public void freeClusterLink(ClusterLink link) {
        if (link.node != null) {
            link.node.link = null;
        }
        link.fd.disconnect(null);
    }
}

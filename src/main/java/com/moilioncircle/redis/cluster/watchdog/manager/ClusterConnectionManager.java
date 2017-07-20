package com.moilioncircle.redis.cluster.watchdog.manager;

import com.moilioncircle.redis.cluster.watchdog.state.ClusterLink;
import com.moilioncircle.redis.cluster.watchdog.state.ClusterNode;

/**
 * Created by Baoyi Chen on 2017/7/12.
 */
public class ClusterConnectionManager {
    public synchronized ClusterLink createClusterLink(ClusterNode node) {
        ClusterLink link = new ClusterLink();
        link.ctime = System.currentTimeMillis();
        link.node = node;
        return link;
    }

    public synchronized void freeClusterLink(ClusterLink link) {
        if (link == null) return;
        if (link.node != null) link.node.link = null;
        if (link.fd != null) link.fd.disconnect(null);
    }
}

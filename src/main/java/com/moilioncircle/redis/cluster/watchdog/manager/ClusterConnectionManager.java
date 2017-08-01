package com.moilioncircle.redis.cluster.watchdog.manager;

import com.moilioncircle.redis.cluster.watchdog.state.ClusterLink;
import com.moilioncircle.redis.cluster.watchdog.state.ClusterNode;

/**
 * @author Leon Chen
 * @since 1.0.0
 */
public class ClusterConnectionManager {

    public synchronized void freeClusterLink(ClusterLink link) {
        if (link == null) return;
        if (link.node != null) link.node.link = null;
        if (link.fd != null) link.fd.disconnect(null);
    }

    public synchronized ClusterLink createClusterLink(ClusterNode node) {
        ClusterLink c = new ClusterLink(); c.node = node; return c;
    }
}

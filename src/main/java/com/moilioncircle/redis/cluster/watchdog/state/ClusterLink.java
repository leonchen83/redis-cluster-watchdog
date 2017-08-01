package com.moilioncircle.redis.cluster.watchdog.state;

import com.moilioncircle.redis.cluster.watchdog.message.RCmbMessage;
import com.moilioncircle.redis.cluster.watchdog.util.net.session.Session;

/**
 * @author Leon Chen
 * @since 1.0.0
 */
public class ClusterLink {
    public volatile long createTime;
    public volatile ClusterNode node;
    public volatile Session<RCmbMessage> fd;
    public ClusterLink() { this.createTime = System.currentTimeMillis(); }
}

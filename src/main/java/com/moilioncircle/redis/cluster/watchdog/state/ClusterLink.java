package com.moilioncircle.redis.cluster.watchdog.state;

import com.moilioncircle.redis.cluster.watchdog.message.RCmbMessage;
import com.moilioncircle.redis.cluster.watchdog.util.net.session.Session;

/**
 * Created by Baoyi Chen on 2017/7/6.
 */
public class ClusterLink {
    public volatile long ctime;
    public volatile ClusterNode node;
    public volatile Session<RCmbMessage> fd;

    @Override
    public String toString() {
        return "ClusterLink{" +
                "ctime=" + ctime +
                ", node=" + node +
                ", fd=" + fd +
                '}';
    }
}

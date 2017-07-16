package com.moilioncircle.replicator.cluster;

import com.moilioncircle.replicator.cluster.message.Message;
import com.moilioncircle.replicator.cluster.util.net.session.Session;

/**
 * Created by Baoyi Chen on 2017/7/6.
 */
public class ClusterLink {
    public volatile long ctime;
    public volatile ClusterNode node;
    public volatile Session<Message> fd;

    @Override
    public String toString() {
        return "ClusterLink{" +
                "ctime=" + ctime +
                ", node=" + node +
                ", fd=" + fd +
                '}';
    }
}

package com.moilioncircle.replicator.cluster;

import com.moilioncircle.replicator.cluster.message.Message;
import com.moilioncircle.replicator.cluster.util.net.session.Session;

/**
 * Created by Baoyi Chen on 2017/7/6.
 */
public class ClusterLink {
    public long ctime;
    public ClusterNode node;
    public Session<Message> fd;
}

package com.moilioncircle.replicator.cluster.gossip;

import com.moilioncircle.replicator.cluster.ClusterLink;
import com.moilioncircle.replicator.cluster.ClusterNode;
import com.moilioncircle.replicator.cluster.ClusterState;
import com.moilioncircle.replicator.cluster.message.Message;
import com.moilioncircle.replicator.cluster.util.net.transport.Transport;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by Baoyi Chen on 2017/7/6.
 */
public class Server {
    public String masterHost;
    public ClusterNode myself;
    public ClusterState cluster;
    public Map<Transport<Message>, ClusterLink> cfd = new ConcurrentHashMap<>();

    public String prevIp;
    public long iteration = 0;
    public long firstCallTime = 0;
    public long amongMinorityTime = 0;
}

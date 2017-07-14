package com.moilioncircle.replicator.cluster.gossip;

import com.moilioncircle.replicator.cluster.ClusterLink;
import com.moilioncircle.replicator.cluster.ClusterState;
import com.moilioncircle.replicator.cluster.message.Message;
import com.moilioncircle.replicator.cluster.util.net.transport.Transport;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by Baoyi Chen on 2017/7/6.
 */
public class Server {
    public ClusterState cluster;
    public String clusterConfigfile;
    public long clusterNodeTimeout;
    public Map<Transport<Message>, ClusterLink> cfd = new ConcurrentHashMap<>();
    public String clusterAnnounceIp;
    public int clusterAnnouncePort;
    public int clusterAnnounceBusPort;
    public int clusterMigrationBarrier;
    public boolean clusterRequireFullCoverage;
    public String masterhost;
    public boolean clusterEnabled;
}

package com.moilioncircle.replicator.cluster.gossip;

import com.moilioncircle.replicator.cluster.ClusterState;
import com.moilioncircle.replicator.cluster.message.Message;
import com.moilioncircle.replicator.cluster.util.net.session.Session;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by Baoyi Chen on 2017/7/6.
 */
public class Server {
    public ClusterState cluster;
    public String clusterConfigfile;
    public long clusterNodeTimeout;
    public int port;
    public List<Session<Message>> cfd = new ArrayList<>();
    public String clusterAnnounceIp;
    public int clusterAnnouncePort;
    public int clusterAnnounceBusPort;
    public long masterReplOffset;
    public int replState;
    public long lastinteraction;
    public long replDownSince;
    public long clusterSlaveValidityFactor;
    public long replPingSlavePeriod;
    public int clusterMigrationBarrier;
    public long clientsPauseEndTime;
    public boolean clusterRequireFullCoverage;
    public String masterhost;
}

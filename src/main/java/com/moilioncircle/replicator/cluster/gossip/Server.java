package com.moilioncircle.replicator.cluster.gossip;

import com.moilioncircle.replicator.cluster.ClusterState;

/**
 * Created by Baoyi Chen on 2017/7/6.
 */
public class Server {
    public ClusterState cluster;
    public String clusterConfigfile;
    public int cfdCount;
    public int port;
}

package com.moilioncircle.replicator.cluster;

import java.util.List;

import static com.moilioncircle.replicator.cluster.ClusterConstants.CLUSTER_SLOTS;

/**
 * Created by Baoyi Chen on 2017/7/6.
 */
public class ClusterNode {
    public int port;
    public int cport;
    public String ip;
    public int flags;
    public long ctime;
    public String name;
    public int numslots;
    public int numslaves;
    public long pingSent;
    public long failTime;
    public long replOffset;
    public ClusterLink link;
    public long configEpoch;
    public long pongReceived;
    public long orphanedTime;
    public ClusterNode slaveof;
    public List<ClusterNode> slaves;
    public List<ClusterNodeFailReport> failReports;
    public byte[] slots = new byte[CLUSTER_SLOTS / 8];
}

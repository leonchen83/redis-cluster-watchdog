package com.moilioncircle.replicator.cluster;

import java.util.ArrayList;
import java.util.List;

import static com.moilioncircle.replicator.cluster.ClusterConstants.CLUSTER_SLOTS;

/**
 * Created by Baoyi Chen on 2017/7/6.
 */
public class ClusterNode {
    public long ctime;
    public String name;
    public int flags;
    public long configEpoch;
    public byte[] slots = new byte[CLUSTER_SLOTS / 8];
    public int numslots;
    public int numslaves;
    public ClusterNode[] slaves;
    public ClusterNode slaveof;
    public long pingSent;
    public long pongReceived;
    public long failTime;
    public long votedTime;
    public long replOffsetTime;
    public long orphanedTime;
    public long replOffset;
    public String ip;
    public int port;
    public int cport;
    public ClusterLink link;
    public List<ClusterNodeFailReport> failReports = new ArrayList<>();
}

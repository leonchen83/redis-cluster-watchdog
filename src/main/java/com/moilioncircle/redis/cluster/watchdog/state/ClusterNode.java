package com.moilioncircle.redis.cluster.watchdog.state;

import java.util.ArrayList;
import java.util.List;

import static com.moilioncircle.redis.cluster.watchdog.ClusterConstants.CLUSTER_SLOTS_BYTES;

/**
 * @author Leon Chen
 * @since 1.0.0
 */
public class ClusterNode {
    public long failTime; public long pingTime;
    public long pongTime; public long votedTime;
    public long createTime; public long isolatedTime;
    public long configEpoch; public int assignedSlots;
    public byte[] slots = new byte[CLUSTER_SLOTS_BYTES];
    public List<ClusterNode> slaves = new ArrayList<>();
    public String ip; public int port; public int busPort;
    public int flags; public String name; public long offset;
    public ClusterNode master; public volatile ClusterLink link;
    public List<ClusterNodeFailReport> failReports = new ArrayList<>();
    public ClusterNode() { this.createTime = System.currentTimeMillis(); }
}

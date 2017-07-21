package com.moilioncircle.redis.cluster.watchdog.state;

import java.util.ArrayList;
import java.util.List;

import static com.moilioncircle.redis.cluster.watchdog.ClusterConstants.CLUSTER_SLOTS_BYTES;

/**
 * @author Leon Chen
 * @since 1.0.0
 */
public class ClusterNode {
    public int port;
    public String ip;
    public int flags;
    public int busPort;
    public String name;
    public long offset;
    public long failTime;
    public long pingTime;
    public long pongTime;
    public long votedTime;
    public long createTime;
    public long configEpoch;
    public int assignedSlots;
    public long isolatedTime;
    public ClusterNode master;
    public volatile ClusterLink link;
    public byte[] slots = new byte[CLUSTER_SLOTS_BYTES];
    public List<ClusterNode> slaves = new ArrayList<>();
    public List<ClusterNodeFailReport> failReports = new ArrayList<>();

    public ClusterNode() {
        this.createTime = System.currentTimeMillis();
    }

    @Override
    public String toString() {
        return "ClusterNode{" +
                "port=" + port +
                ", busPort=" + busPort +
                ", ip='" + ip + '\'' +
                ", flags=" + flags +
                ", createTime=" + createTime +
                ", name='" + name + '\'' +
                ", pingTime=" + pingTime +
                ", failTime=" + failTime +
                ", offset=" + offset +
                ", configEpoch=" + configEpoch +
                ", pongTime=" + pongTime +
                ", isolatedTime=" + isolatedTime +
                '}';
    }
}

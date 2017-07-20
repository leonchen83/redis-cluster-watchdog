package com.moilioncircle.redis.cluster.watchdog.state;

import com.moilioncircle.redis.cluster.watchdog.ClusterConstants;

import java.util.ArrayList;
import java.util.List;

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
    public long votedTime;
    public long replOffset;
    public long configEpoch;
    public long pongReceived;
    public long orphanedTime;
    public ClusterNode slaveof;
    public volatile ClusterLink link;
    public byte[] slots = new byte[ClusterConstants.CLUSTER_SLOTS / 8];
    public List<ClusterNode> slaves = new ArrayList<>();
    public List<ClusterNodeFailReport> failReports = new ArrayList<>();

    @Override
    public String toString() {
        return "ClusterNode{" +
                "port=" + port +
                ", cport=" + cport +
                ", ip='" + ip + '\'' +
                ", flags=" + flags +
                ", ctime=" + ctime +
                ", name='" + name + '\'' +
                ", numslaves=" + numslaves +
                ", pingSent=" + pingSent +
                ", failTime=" + failTime +
                ", replOffset=" + replOffset +
                ", configEpoch=" + configEpoch +
                ", pongReceived=" + pongReceived +
                ", orphanedTime=" + orphanedTime +
                '}';
    }
}

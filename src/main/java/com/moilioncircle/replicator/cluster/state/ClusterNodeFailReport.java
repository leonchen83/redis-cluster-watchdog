package com.moilioncircle.replicator.cluster.state;

/**
 * Created by Baoyi Chen on 2017/7/6.
 */
public class ClusterNodeFailReport {
    public long time;
    public ClusterNode node;

    @Override
    public String toString() {
        return "ClusterNodeFailReport{" +
                "time=" + time +
                ", node=" + node +
                '}';
    }
}

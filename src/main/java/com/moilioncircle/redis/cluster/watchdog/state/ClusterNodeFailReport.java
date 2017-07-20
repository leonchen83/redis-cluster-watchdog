package com.moilioncircle.redis.cluster.watchdog.state;

/**
 * Created by Baoyi Chen on 2017/7/6.
 */
public class ClusterNodeFailReport {
    public long time;
    public ClusterNode node;

    public ClusterNodeFailReport(ClusterNode node) {
        this.time = System.currentTimeMillis();
        this.node = node;
    }

    @Override
    public String toString() {
        return "ClusterNodeFailReport{" +
                "time=" + time +
                ", node=" + node +
                '}';
    }
}

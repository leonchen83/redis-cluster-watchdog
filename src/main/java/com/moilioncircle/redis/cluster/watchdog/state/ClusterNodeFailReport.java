package com.moilioncircle.redis.cluster.watchdog.state;

/**
 * @author Leon Chen
 * @since 1.0.0
 */
public class ClusterNodeFailReport {
    public long createTime;
    public ClusterNode node;

    public ClusterNodeFailReport(ClusterNode node) {
        this.createTime = System.currentTimeMillis();
        this.node = node;
    }
}

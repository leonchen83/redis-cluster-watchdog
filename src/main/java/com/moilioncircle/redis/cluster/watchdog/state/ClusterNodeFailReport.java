package com.moilioncircle.redis.cluster.watchdog.state;

/**
 * @author Leon Chen
 * @since 1.0.0
 */
public class ClusterNodeFailReport {
    public ClusterNode node;
    public long createTime = System.currentTimeMillis();
    public ClusterNodeFailReport(ClusterNode node) { this.node = node; }
}

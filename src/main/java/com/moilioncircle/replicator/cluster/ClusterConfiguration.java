package com.moilioncircle.replicator.cluster;

import static com.moilioncircle.replicator.cluster.ClusterConstants.CLUSTER_PORT_INCR;

/**
 * Created by Baoyi Chen on 2017/7/6.
 */
public class ClusterConfiguration {

    private int clusterAnnouncePort;
    private String clusterAnnounceIp;
    private String clusterConfigfile;
    private int clusterAnnounceBusPort;
    private long clusterNodeTimeout = 15000;
    private int clusterMigrationBarrier = 1;
    private boolean clusterRequireFullCoverage = true;

    public String getClusterAnnounceIp() {
        return clusterAnnounceIp;
    }

    public void setClusterAnnounceIp(String clusterAnnounceIp) {
        this.clusterAnnounceIp = clusterAnnounceIp;
    }

    public long getClusterNodeTimeout() {
        return clusterNodeTimeout;
    }

    public void setClusterNodeTimeout(long clusterNodeTimeout) {
        this.clusterNodeTimeout = clusterNodeTimeout;
    }

    public int getClusterAnnouncePort() {
        return clusterAnnouncePort;
    }

    public void setClusterAnnouncePort(int clusterAnnouncePort) {
        this.clusterAnnouncePort = clusterAnnouncePort;
    }

    public String getClusterConfigfile() {
        return clusterConfigfile;
    }

    public void setClusterConfigfile(String clusterConfigfile) {
        this.clusterConfigfile = clusterConfigfile;
    }

    public int getClusterMigrationBarrier() {
        return clusterMigrationBarrier;
    }

    public void setClusterMigrationBarrier(int clusterMigrationBarrier) {
        this.clusterMigrationBarrier = clusterMigrationBarrier;
    }

    public int getClusterAnnounceBusPort() {
        return clusterAnnounceBusPort;
    }

    public void setClusterAnnounceBusPort(int clusterAnnounceBusPort) {
        this.clusterAnnounceBusPort = clusterAnnounceBusPort;
    }

    public boolean isClusterRequireFullCoverage() {
        return clusterRequireFullCoverage;
    }

    public void setClusterRequireFullCoverage(boolean clusterRequireFullCoverage) {
        this.clusterRequireFullCoverage = clusterRequireFullCoverage;
    }

    public void validate() {
        if (clusterAnnouncePort <= 0 || clusterAnnouncePort > 65535) {
            throw new ConfigurationException("illegal port" + clusterAnnouncePort);
        }

        if (clusterAnnounceBusPort == 0) {
            clusterAnnounceBusPort = clusterAnnouncePort + CLUSTER_PORT_INCR;
        }

        if (clusterAnnounceBusPort <= 0 || clusterAnnounceBusPort > 65535) {
            throw new ConfigurationException("illegal bus port" + clusterAnnounceBusPort);
        }

        if (clusterConfigfile == null) {
            clusterConfigfile = "nodes-" + clusterAnnouncePort + ".conf";
        }

        if (clusterMigrationBarrier < 1) {
            throw new ConfigurationException("illegal migration barrier" + clusterMigrationBarrier);
        }

        if (clusterNodeTimeout <= 0) {
            throw new ConfigurationException("illegal node timeout" + clusterNodeTimeout);
        }
    }
}

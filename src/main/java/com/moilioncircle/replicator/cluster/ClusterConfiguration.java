package com.moilioncircle.replicator.cluster;

/**
 * Created by Baoyi Chen on 2017/7/6.
 */
public class ClusterConfiguration {

    private boolean clusterEnabled;
    private long clusterNodeTimeout;
    private int clusterAnnouncePort;
    private String clusterConfigfile;
    private String clusterAnnounceIp;
    private int clusterAnnounceBusPort;
    private int clusterMigrationBarrier;
    private boolean clusterRequireFullCoverage;

    public boolean isClusterEnabled() {
        return clusterEnabled;
    }

    public void setClusterEnabled(boolean clusterEnabled) {
        this.clusterEnabled = clusterEnabled;
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

    public String getClusterAnnounceIp() {
        return clusterAnnounceIp;
    }

    public void setClusterAnnounceIp(String clusterAnnounceIp) {
        this.clusterAnnounceIp = clusterAnnounceIp;
    }

    public int getClusterAnnounceBusPort() {
        return clusterAnnounceBusPort;
    }

    public void setClusterAnnounceBusPort(int clusterAnnounceBusPort) {
        this.clusterAnnounceBusPort = clusterAnnounceBusPort;
    }

    public int getClusterMigrationBarrier() {
        return clusterMigrationBarrier;
    }

    public void setClusterMigrationBarrier(int clusterMigrationBarrier) {
        this.clusterMigrationBarrier = clusterMigrationBarrier;
    }

    public boolean isClusterRequireFullCoverage() {
        return clusterRequireFullCoverage;
    }

    public void setClusterRequireFullCoverage(boolean clusterRequireFullCoverage) {
        this.clusterRequireFullCoverage = clusterRequireFullCoverage;
    }
}

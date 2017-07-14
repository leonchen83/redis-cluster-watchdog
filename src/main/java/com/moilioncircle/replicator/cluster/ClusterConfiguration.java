package com.moilioncircle.replicator.cluster;

/**
 * Created by Baoyi Chen on 2017/7/6.
 */
public class ClusterConfiguration {

    private long clusterNodeTimeout = 15000;
    private int clusterAnnouncePort = 6379;
    private String clusterConfigfile = "nodes-6379.conf";
    private String clusterAnnounceIp;
    private int clusterAnnounceBusPort = 6380;
    private int clusterMigrationBarrier = 1;
    private boolean clusterRequireFullCoverage = true;

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

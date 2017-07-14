package com.moilioncircle.replicator.cluster;

/**
 * Created by Baoyi Chen on 2017/7/6.
 */
public class ClusterConfiguration {

    private String clusterAnnounceIp;
    private long clusterNodeTimeout = 15000;
    private int clusterAnnouncePort = 6379;
    private int clusterMigrationBarrier = 1;
    private int clusterAnnounceBusPort = 6380;
    private boolean clusterRequireFullCoverage = true;
    private String selfName = "redis-cluster-watchdog-00000000000000001";

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

    public String getSelfName() {
        return selfName;
    }

    public void setSelfName(String selfName) {
        this.selfName = selfName;
    }
}

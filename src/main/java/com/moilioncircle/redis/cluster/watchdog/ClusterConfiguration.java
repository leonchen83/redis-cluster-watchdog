package com.moilioncircle.redis.cluster.watchdog;

/**
 * Created by Baoyi Chen on 2017/7/6.
 */
public class ClusterConfiguration {

    private String clusterAnnounceIp;
    private String clusterConfigfile;
    private int clusterAnnounceBusPort;
    private int clusterAnnouncePort = 6379;
    private long clusterNodeTimeout = 15000;
    private int clusterMigrationBarrier = 1;
    private boolean clusterRequireFullCoverage = true;

    public String getClusterAnnounceIp() {
        return clusterAnnounceIp;
    }

    public ClusterConfiguration setClusterAnnounceIp(String clusterAnnounceIp) {
        this.clusterAnnounceIp = clusterAnnounceIp;
        return this;
    }

    public long getClusterNodeTimeout() {
        return clusterNodeTimeout;
    }

    public ClusterConfiguration setClusterNodeTimeout(long clusterNodeTimeout) {
        this.clusterNodeTimeout = clusterNodeTimeout;
        return this;
    }

    public int getClusterAnnouncePort() {
        return clusterAnnouncePort;
    }

    public ClusterConfiguration setClusterAnnouncePort(int clusterAnnouncePort) {
        this.clusterAnnouncePort = clusterAnnouncePort;
        return this;
    }

    public String getClusterConfigfile() {
        return clusterConfigfile;
    }

    public ClusterConfiguration setClusterConfigfile(String clusterConfigfile) {
        this.clusterConfigfile = clusterConfigfile;
        return this;
    }

    public int getClusterMigrationBarrier() {
        return clusterMigrationBarrier;
    }

    public ClusterConfiguration setClusterMigrationBarrier(int clusterMigrationBarrier) {
        this.clusterMigrationBarrier = clusterMigrationBarrier;
        return this;
    }

    public int getClusterAnnounceBusPort() {
        return clusterAnnounceBusPort;
    }

    public ClusterConfiguration setClusterAnnounceBusPort(int clusterAnnounceBusPort) {
        this.clusterAnnounceBusPort = clusterAnnounceBusPort;
        return this;
    }

    public boolean isClusterRequireFullCoverage() {
        return clusterRequireFullCoverage;
    }

    public ClusterConfiguration setClusterRequireFullCoverage(boolean clusterRequireFullCoverage) {
        this.clusterRequireFullCoverage = clusterRequireFullCoverage;
        return this;
    }

    public void validate() {
        if (clusterAnnouncePort <= 0 || clusterAnnouncePort > 65535) {
            throw new ConfigurationException("illegal port" + clusterAnnouncePort);
        }

        if (clusterAnnounceBusPort == 0) {
            clusterAnnounceBusPort = clusterAnnouncePort + ClusterConstants.CLUSTER_PORT_INCR;
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

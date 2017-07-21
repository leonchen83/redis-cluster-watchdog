package com.moilioncircle.redis.cluster.watchdog;

/**
 * @author Leon Chen
 * @since 1.0.0
 */
public class ClusterConfiguration {

    private boolean verbose = false;
    private String clusterAnnounceIp;
    private String clusterConfigFile;
    private int clusterAnnounceBusPort;
    private int clusterAnnouncePort = 6379;
    private long clusterNodeTimeout = 15000;
    private int clusterMigrationBarrier = 1;
    private boolean clusterRequireFullCoverage = true;

    private ClusterConfiguration() {
    }

    public static ClusterConfiguration defaultSetting() {
        return new ClusterConfiguration();
    }

    public boolean isVerbose() {
        return verbose;
    }

    public ClusterConfiguration setVerbose(boolean verbose) {
        this.verbose = verbose;
        validate();
        return this;
    }

    public String getClusterAnnounceIp() {
        return clusterAnnounceIp;
    }

    public ClusterConfiguration setClusterAnnounceIp(String clusterAnnounceIp) {
        this.clusterAnnounceIp = clusterAnnounceIp;
        validate();
        return this;
    }

    public long getClusterNodeTimeout() {
        return clusterNodeTimeout;
    }

    public ClusterConfiguration setClusterNodeTimeout(long clusterNodeTimeout) {
        this.clusterNodeTimeout = clusterNodeTimeout;
        validate();
        return this;
    }

    public int getClusterAnnouncePort() {
        return clusterAnnouncePort;
    }

    public ClusterConfiguration setClusterAnnouncePort(int clusterAnnouncePort) {
        this.clusterAnnouncePort = clusterAnnouncePort;
        validate();
        return this;
    }

    public String getClusterConfigFile() {
        return clusterConfigFile;
    }

    public ClusterConfiguration setClusterConfigFile(String clusterConfigFile) {
        this.clusterConfigFile = clusterConfigFile;
        validate();
        return this;
    }

    public int getClusterMigrationBarrier() {
        return clusterMigrationBarrier;
    }

    public ClusterConfiguration setClusterMigrationBarrier(int clusterMigrationBarrier) {
        this.clusterMigrationBarrier = clusterMigrationBarrier;
        validate();
        return this;
    }

    public int getClusterAnnounceBusPort() {
        return clusterAnnounceBusPort;
    }

    public ClusterConfiguration setClusterAnnounceBusPort(int clusterAnnounceBusPort) {
        this.clusterAnnounceBusPort = clusterAnnounceBusPort;
        validate();
        return this;
    }

    public boolean isClusterRequireFullCoverage() {
        return clusterRequireFullCoverage;
    }

    public ClusterConfiguration setClusterRequireFullCoverage(boolean clusterRequireFullCoverage) {
        this.clusterRequireFullCoverage = clusterRequireFullCoverage;
        validate();
        return this;
    }

    private void validate() {
        if (clusterAnnouncePort <= 0 || clusterAnnouncePort > 65535) {
            throw new ConfigurationException("illegal port" + clusterAnnouncePort);
        }

        if (clusterAnnounceBusPort == 0) {
            clusterAnnounceBusPort = clusterAnnouncePort + ClusterConstants.CLUSTER_PORT_INCR;
        }

        if (clusterAnnounceBusPort <= 0 || clusterAnnounceBusPort > 65535) {
            throw new ConfigurationException("illegal bus port" + clusterAnnounceBusPort);
        }

        if (clusterConfigFile == null) {
            clusterConfigFile = "nodes-" + clusterAnnouncePort + ".conf";
        }

        if (clusterMigrationBarrier < 1) {
            throw new ConfigurationException("illegal migration barrier" + clusterMigrationBarrier);
        }

        if (clusterNodeTimeout <= 0) {
            throw new ConfigurationException("illegal node timeout" + clusterNodeTimeout);
        }
    }
}

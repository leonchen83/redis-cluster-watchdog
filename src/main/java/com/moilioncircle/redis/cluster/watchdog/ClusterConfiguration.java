package com.moilioncircle.redis.cluster.watchdog;

import com.moilioncircle.redis.cluster.watchdog.util.net.NioBootstrapConfiguration;

/**
 * @author Leon Chen
 * @since 1.0.0
 */
public class ClusterConfiguration {

    private boolean verbose = false;
    private boolean asMaster = false;
    private String clusterAnnounceIp;
    private String clusterConfigFile;
    private int clusterAnnounceBusPort;
    private int clusterAnnouncePort = 6379;
    private long clusterNodeTimeout = 15000;
    private int clusterMigrationBarrier = 1;
    private boolean clusterRequireFullCoverage = true;
    private NioBootstrapConfiguration networkConfiguration = NioBootstrapConfiguration.defaultSetting();

    private ClusterConfiguration() {
    }

    public static ClusterConfiguration defaultSetting() {
        ClusterConfiguration configuration = new ClusterConfiguration();
        configuration.validate();
        return configuration;
    }

    public boolean isVerbose() {
        return verbose;
    }

    public ClusterConfiguration setVerbose(boolean verbose) {
        this.verbose = verbose;
        validate();
        return this;
    }

    public boolean isAsMaster() {
        return asMaster;
    }

    public ClusterConfiguration setAsMaster(boolean asMaster) {
        this.asMaster = asMaster;
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

    public NioBootstrapConfiguration getNetworkConfiguration() {
        return networkConfiguration;
    }

    public ClusterConfiguration setNetworkConfiguration(NioBootstrapConfiguration networkConfiguration) {
        this.networkConfiguration = networkConfiguration;
        validate();
        return this;
    }

    private void validate() {
        if (clusterAnnouncePort <= 0 || clusterAnnouncePort > 65535) {
            throw new ClusterConfigurationException("illegal port" + clusterAnnouncePort);
        }

        if (clusterAnnounceBusPort == 0) {
            clusterAnnounceBusPort = clusterAnnouncePort + ClusterConstants.CLUSTER_PORT_INCR;
        }

        if (clusterAnnounceBusPort <= 0 || clusterAnnounceBusPort > 65535) {
            throw new ClusterConfigurationException("illegal bus port" + clusterAnnounceBusPort);
        }

        if (clusterConfigFile == null) {
            clusterConfigFile = "nodes-" + clusterAnnouncePort + ".conf";
        }

        if (clusterMigrationBarrier < 1) {
            throw new ClusterConfigurationException("illegal migration barrier" + clusterMigrationBarrier);
        }

        if (clusterNodeTimeout <= 0) {
            throw new ClusterConfigurationException("illegal node timeout" + clusterNodeTimeout);
        }
    }
}

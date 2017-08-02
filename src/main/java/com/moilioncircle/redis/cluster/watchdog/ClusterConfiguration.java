package com.moilioncircle.redis.cluster.watchdog;

import com.moilioncircle.redis.cluster.watchdog.util.net.NetworkConfiguration;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import static com.moilioncircle.redis.cluster.watchdog.ClusterConstants.CLUSTER_PORT_INCR;
import static com.moilioncircle.redis.cluster.watchdog.Version.PROTOCOL_V0;

/**
 * @author Leon Chen
 * @since 1.0.0
 */
public class ClusterConfiguration {

    private static final Log logger = LogFactory.getLog(ClusterConfiguration.class);

    private String clusterAnnounceIp;
    private String clusterConfigFile;
    private int clusterAnnounceBusPort;
    private int clusterAnnouncePort = 6379;
    private volatile boolean verbose = false;
    private volatile boolean failover = false;
    private volatile Version version = PROTOCOL_V0;
    private volatile int clusterMigrationBarrier = 1;
    private volatile long clusterNodeTimeout = 15000;
    private volatile boolean clusterFullCoverage = true;
    private NetworkConfiguration networkConfiguration = NetworkConfiguration.defaultSetting();

    private ClusterConfiguration() {
    }

    public static ClusterConfiguration defaultSetting() {
        return new ClusterConfiguration();
    }

    /**
     *
     */
    public boolean isVerbose() {
        return verbose;
    }

    public boolean isFailover() {
        return failover;
    }

    public Version getVersion() {
        return version;
    }

    public long getClusterNodeTimeout() {
        return clusterNodeTimeout;
    }

    public int getClusterAnnouncePort() {
        return clusterAnnouncePort;
    }

    public String getClusterAnnounceIp() {
        return clusterAnnounceIp;
    }

    public String getClusterConfigFile() {
        return clusterConfigFile;
    }

    public int getClusterAnnounceBusPort() {
        return clusterAnnounceBusPort;
    }

    public boolean isClusterFullCoverage() {
        return clusterFullCoverage;
    }

    public int getClusterMigrationBarrier() {
        return clusterMigrationBarrier;
    }

    public NetworkConfiguration getNetworkConfiguration() {
        return networkConfiguration;
    }

    /**
     *
     */
    public ClusterConfiguration setVerbose(boolean verbose) {
        this.verbose = verbose;
        return this;
    }

    public ClusterConfiguration setVersion(Version version) {
        this.version = version;
        return this;
    }

    public ClusterConfiguration setFailover(boolean failover) {
        this.failover = failover;
        return this;
    }

    public ClusterConfiguration setClusterAnnounceIp(String clusterAnnounceIp) {
        this.clusterAnnounceIp = clusterAnnounceIp;
        return this;
    }

    public ClusterConfiguration setClusterNodeTimeout(long clusterNodeTimeout) {
        this.clusterNodeTimeout = clusterNodeTimeout;
        return this;
    }

    public ClusterConfiguration setClusterConfigFile(String clusterConfigFile) {
        this.clusterConfigFile = clusterConfigFile;
        return this;
    }

    public ClusterConfiguration setClusterAnnouncePort(int clusterAnnouncePort) {
        this.clusterAnnouncePort = clusterAnnouncePort;
        return this;
    }

    public ClusterConfiguration setClusterFullCoverage(boolean clusterFullCoverage) {
        this.clusterFullCoverage = clusterFullCoverage;
        return this;
    }

    public ClusterConfiguration setClusterAnnounceBusPort(int clusterAnnounceBusPort) {
        this.clusterAnnounceBusPort = clusterAnnounceBusPort;
        return this;
    }

    public ClusterConfiguration setClusterMigrationBarrier(int clusterMigrationBarrier) {
        this.clusterMigrationBarrier = clusterMigrationBarrier;
        return this;
    }

    public ClusterConfiguration setNetworkConfiguration(NetworkConfiguration networkConfiguration) {
        this.networkConfiguration = networkConfiguration;
        return this;
    }

    /**
     *
     */
    ClusterConfiguration validate() {
        if (version == null) {
            throw new ClusterConfigurationException("illegal version: " + version);
        }

        if (clusterAnnouncePort <= 0 || clusterAnnouncePort > 65535) {
            throw new ClusterConfigurationException("illegal clusterAnnouncePort: " + clusterAnnouncePort);
        }

        if (clusterAnnounceBusPort == 0 || version == PROTOCOL_V0) {
            clusterAnnounceBusPort = clusterAnnouncePort + CLUSTER_PORT_INCR;
            if (version == PROTOCOL_V0)
                logger.warn("clusterAnnounceBusPort force set to " + clusterAnnounceBusPort + ", cause cluster protocol version is 0.");
        }

        if (clusterAnnounceBusPort <= 0 || clusterAnnounceBusPort > 65535) {
            throw new ClusterConfigurationException("illegal clusterAnnounceBusPort: " + clusterAnnounceBusPort);
        }

        if (clusterAnnounceIp != null && version == PROTOCOL_V0) {
            clusterAnnounceIp = null; logger.warn("clusterAnnounceIp force set to null, cause version is 0.");
        }

        if (clusterConfigFile == null) {
            clusterConfigFile = "nodes-" + clusterAnnouncePort + ".conf";
        }

        if (clusterMigrationBarrier < 1) {
            throw new ClusterConfigurationException("illegal clusterMigrationBarrier: " + clusterMigrationBarrier);
        }

        if (clusterNodeTimeout <= 0) {
            throw new ClusterConfigurationException("illegal clusterNodeTimeout: " + clusterNodeTimeout);
        }

        return this;
    }
}

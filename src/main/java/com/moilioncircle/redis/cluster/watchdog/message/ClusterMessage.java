package com.moilioncircle.redis.cluster.watchdog.message;

import com.moilioncircle.redis.cluster.watchdog.ClusterState;
import com.moilioncircle.redis.cluster.watchdog.Version;

import static com.moilioncircle.redis.cluster.watchdog.ClusterConstants.CLUSTER_SLOTS_BYTES;

/**
 * @author Leon Chen
 * @since 1.0.0
 */
public class ClusterMessage implements RCmbMessage {
    public int type;
    public int port;
    public String ip;
    public int flags;
    public int count;
    public int length;
    public String name;
    public int busPort;
    public long offset;
    public String master;
    public Version version;
    public String signature;
    public long configEpoch;
    public long currentEpoch;
    public ClusterState state;
    public byte[] messageFlags = new byte[3];
    public byte[] slots = new byte[CLUSTER_SLOTS_BYTES];
    public ClusterMessageData data = new ClusterMessageData();
}

package com.moilioncircle.redis.cluster.watchdog.message;

import com.moilioncircle.redis.cluster.watchdog.ClusterState;
import com.moilioncircle.redis.cluster.watchdog.Version;

import static com.moilioncircle.redis.cluster.watchdog.ClusterConstants.CLUSTER_SLOTS_BYTES;

/**
 * @author Leon Chen
 * @since 1.0.0
 */
public class ClusterMessage implements RCmbMessage {
    public byte[] messageFlags = new byte[3];
    public String master; public Version version;
    public String signature; public long configEpoch;
    public long currentEpoch; public ClusterState state;
    public byte[] slots = new byte[CLUSTER_SLOTS_BYTES];
    public int type; public int flags; public String name;
    public String ip; public int port; public int busPort;
    public int count; public int length; public long offset;
    public ClusterMessageData data = new ClusterMessageData();
}

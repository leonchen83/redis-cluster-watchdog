package com.moilioncircle.redis.cluster.watchdog.message;

import static com.moilioncircle.redis.cluster.watchdog.ClusterConstants.CLUSTER_SLOTS_BYTES;

/**
 * @author Leon Chen
 * @since 1.0.0
 */
public class ClusterMessageDataUpdate {
    public String name;
    public long configEpoch;
    public byte[] slots = new byte[CLUSTER_SLOTS_BYTES];

    @Override
    public String toString() {
        return "ClusterMessageDataUpdate{" + "configEpoch=" + configEpoch + ", name='" + name + '\'' + '}';
    }
}

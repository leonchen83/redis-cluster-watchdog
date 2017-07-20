package com.moilioncircle.redis.cluster.watchdog.message;

import static com.moilioncircle.redis.cluster.watchdog.ClusterConstants.CLUSTER_SLOTS;

/**
 * Created by Baoyi Chen on 2017/7/6.
 */
public class ClusterMessageDataUpdate {
    public long configEpoch;
    public String nodename;
    public byte[] slots = new byte[CLUSTER_SLOTS / 8];

    @Override
    public String toString() {
        return "ClusterMessageDataUpdate{" +
                "configEpoch=" + configEpoch +
                ", nodename='" + nodename + '\'' +
                '}';
    }
}

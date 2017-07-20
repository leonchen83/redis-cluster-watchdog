package com.moilioncircle.redis.cluster.watchdog.message;

import com.moilioncircle.redis.cluster.watchdog.ClusterConstants;

/**
 * Created by Baoyi Chen on 2017/7/6.
 */
public class ClusterMessageDataUpdate {
    public long configEpoch;
    public String nodename;
    public byte[] slots = new byte[ClusterConstants.CLUSTER_SLOTS / 8];

    @Override
    public String toString() {
        return "ClusterMessageDataUpdate{" +
                "configEpoch=" + configEpoch +
                ", nodename='" + nodename + '\'' +
                '}';
    }
}

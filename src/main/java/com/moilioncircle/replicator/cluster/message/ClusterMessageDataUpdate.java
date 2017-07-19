package com.moilioncircle.replicator.cluster.message;

import static com.moilioncircle.replicator.cluster.ClusterConstants.CLUSTER_SLOTS;

/**
 * Created by Baoyi Chen on 2017/7/6.
 */
public class ClusterMessageDataUpdate {
    public long configEpoch;
    public String nodename;
    public byte[] slots = new byte[CLUSTER_SLOTS / 8];

    @Override
    public String toString() {
        return "ClusterMsgDataUpdate{" +
                "configEpoch=" + configEpoch +
                ", nodename='" + nodename + '\'' +
//                ", slots=" + Arrays.toString(slots) +
                '}';
    }
}

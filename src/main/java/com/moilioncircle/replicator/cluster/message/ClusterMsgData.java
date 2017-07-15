package com.moilioncircle.replicator.cluster.message;

import java.util.Arrays;

/**
 * Created by Baoyi Chen on 2017/7/6.
 */
public class ClusterMsgData {
    public ClusterMsgDataGossip[] gossip = new ClusterMsgDataGossip[0];
    public ClusterMsgDataFail about;
    public ClusterMsgDataPublish msg;
    public ClusterMsgDataUpdate nodecfg;

    @Override
    public String toString() {
        return "ClusterMsgData{" +
                "gossip=" + Arrays.toString(gossip) +
                ", about=" + about +
                ", msg=" + msg +
                ", nodecfg=" + nodecfg +
                '}';
    }
}

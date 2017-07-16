package com.moilioncircle.replicator.cluster.message;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by Baoyi Chen on 2017/7/6.
 */
public class ClusterMsgData {
    public List<ClusterMsgDataGossip> gossip = new ArrayList<>();
    public ClusterMsgDataFail about;
    public ClusterMsgDataPublish msg;
    public ClusterMsgDataUpdate nodecfg;

    @Override
    public String toString() {
        return "ClusterMsgData{" +
                "gossip=" + gossip +
                ", about=" + about +
                ", msg=" + msg +
                ", nodecfg=" + nodecfg +
                '}';
    }
}

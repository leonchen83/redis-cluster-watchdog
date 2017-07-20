package com.moilioncircle.redis.cluster.watchdog.message;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by Baoyi Chen on 2017/7/6.
 */
public class ClusterMessageData {
    public List<ClusterMessageDataGossip> gossip = new ArrayList<>();
    public ClusterMessageDataFail about;
    public ClusterMessageDataPublish msg;
    public ClusterMessageDataUpdate nodecfg;

    @Override
    public String toString() {
        return "ClusterMessageData{" +
                "gossip=" + gossip +
                ", about=" + about +
                ", msg=" + msg +
                ", nodecfg=" + nodecfg +
                '}';
    }
}

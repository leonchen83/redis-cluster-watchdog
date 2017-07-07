package com.moilioncircle.replicator.cluster.message;

/**
 * Created by Baoyi Chen on 2017/7/6.
 */
public class ClusterMsgData {
    public ClusterMsgDataGossip[] gossip;
    public ClusterMsgDataFail about;
    public ClusterMsgDataPublish msg;
    public ClusterMsgDataUpdate nodecfg;
}

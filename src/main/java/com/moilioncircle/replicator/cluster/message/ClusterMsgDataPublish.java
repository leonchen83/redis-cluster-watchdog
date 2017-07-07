package com.moilioncircle.replicator.cluster.message;

/**
 * Created by Baoyi Chen on 2017/7/6.
 */
public class ClusterMsgDataPublish {
    public int channelLen;
    public int messageLen;
    public byte[] bulkData = new byte[8];
}

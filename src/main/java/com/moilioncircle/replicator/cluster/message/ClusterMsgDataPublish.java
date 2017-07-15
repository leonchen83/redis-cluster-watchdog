package com.moilioncircle.replicator.cluster.message;

import java.util.Arrays;

/**
 * Created by Baoyi Chen on 2017/7/6.
 */
public class ClusterMsgDataPublish {
    public int channelLen;
    public int messageLen;
    public byte[] bulkData = new byte[8];

    @Override
    public String toString() {
        return "ClusterMsgDataPublish{" +
                "channelLen=" + channelLen +
                ", messageLen=" + messageLen +
                ", bulkData=" + Arrays.toString(bulkData) +
                '}';
    }
}

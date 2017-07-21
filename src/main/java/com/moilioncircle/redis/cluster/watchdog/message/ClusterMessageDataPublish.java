package com.moilioncircle.redis.cluster.watchdog.message;

import java.util.Arrays;

/**
 * @author Leon Chen
 * @since 1.0.0
 */
public class ClusterMessageDataPublish {
    public int channelLength;
    public int messageLength;
    public byte[] bulkData = new byte[8];

    @Override
    public String toString() {
        return "ClusterMessageDataPublish{" +
                "channelLength=" + channelLength +
                ", messageLength=" + messageLength +
                ", bulkData=" + Arrays.toString(bulkData) +
                '}';
    }
}

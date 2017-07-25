package com.moilioncircle.redis.cluster.watchdog.message;

/**
 * @author Leon Chen
 * @since 1.0.0
 */
public class ClusterMessageDataPublish {
    public int channelLength;
    public int messageLength;
    public byte[] bulkData = new byte[8];
}

package com.moilioncircle.redis.cluster.watchdog.message;

/**
 * @author Leon Chen
 * @since 1.0.0
 */
public class ClusterMessageDataPublish {
    public byte[] bulkData = new byte[8];
    public int channelLength; public int messageLength;
}

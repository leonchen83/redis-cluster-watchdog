package com.moilioncircle.redis.cluster.watchdog.message;

/**
 * @author Leon Chen
 * @since 1.0.0
 */
public class ClusterMessageDataGossip {
    public int port;
    public int flags;
    public String ip;
    public String name;
    public int busPort;
    public long pingTime;
    public long pongTime;
    public byte[] reserved = new byte[4];

    @Override
    public String toString() {
        return "ClusterMessageDataGossip{" +
                "name='" + name + '\'' +
                ", pingTime=" + pingTime +
                ", pongTime=" + pongTime +
                ", ip='" + ip + '\'' +
                ", port=" + port +
                ", busPort=" + busPort +
                ", flags=" + flags +
                '}';
    }
}

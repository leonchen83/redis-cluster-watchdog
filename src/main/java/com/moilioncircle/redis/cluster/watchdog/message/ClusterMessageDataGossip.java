package com.moilioncircle.redis.cluster.watchdog.message;

/**
 * @author Leon Chen
 * @since 1.0.0
 */
public class ClusterMessageDataGossip {
    public int flags; public String name;
    public long pingTime; public long pongTime;
    public String ip; public int port; public int busPort;
}

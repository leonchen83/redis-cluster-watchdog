package com.moilioncircle.replicator.cluster.message;

/**
 * Created by Baoyi Chen on 2017/7/6.
 */
public class ClusterMsgDataGossip {
    public String nodename;
    public long pingSent;
    public long pongReceived;
    public String ip;
    public int port;
    public int cport;
    public int flags;
    public int notused1;
}

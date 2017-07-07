package com.moilioncircle.replicator.cluster.message;

/**
 * Created by Baoyi Chen on 2017/7/6.
 */
public class ClusterMsg implements Message {
    public String sig;
    public int totlen;
    public int ver;
    public int port;
    public int type;
    public int count;
    public long currentEpoch;
    public long configEpoch;
    public long offset;
    public String sender;
    public String myslots;
    public String slaveof;
    public String myip;
    public byte[] notused = new byte[34];
    public int cport;
    public int flags;
    public int state;
    public int mflags;
    public ClusterMsgData data;
}

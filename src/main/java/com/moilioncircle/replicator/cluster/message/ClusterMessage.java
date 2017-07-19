package com.moilioncircle.replicator.cluster.message;

import java.util.Arrays;

import static com.moilioncircle.replicator.cluster.ClusterConstants.CLUSTER_SLOTS;

/**
 * Created by Baoyi Chen on 2017/7/6.
 */
public class ClusterMessage implements RCmbMessage {
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
    public byte[] myslots = new byte[CLUSTER_SLOTS / 8];
    public String slaveof;
    public String myip;
    public byte[] notused = new byte[34];
    public int cport;
    public int flags;
    public byte state;
    public byte[] mflags = new byte[3];
    public ClusterMessageData data;

    @Override
    public String toString() {
        return "ClusterMessage{" +
                "sig='" + sig + '\'' +
                ", totlen=" + totlen +
                ", ver=" + ver +
                ", port=" + port +
                ", type=" + type +
                ", count=" + count +
                ", currentEpoch=" + currentEpoch +
                ", configEpoch=" + configEpoch +
                ", offset=" + offset +
                ", sender='" + sender + '\'' +
                ", slaveof='" + slaveof + '\'' +
                ", myip='" + myip + '\'' +
                ", cport=" + cport +
                ", flags=" + flags +
                ", state=" + state +
                ", mflags=" + Arrays.toString(mflags) +
                ", data=" + data +
                '}';
    }
}

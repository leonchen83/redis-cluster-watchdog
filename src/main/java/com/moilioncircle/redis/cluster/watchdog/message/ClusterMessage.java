package com.moilioncircle.redis.cluster.watchdog.message;

import com.moilioncircle.redis.cluster.watchdog.ClusterState;

import java.util.Arrays;

import static com.moilioncircle.redis.cluster.watchdog.ClusterConstants.CLUSTER_SLOTS_BYTES;

/**
 * @author Leon Chen
 * @since 1.0.0
 */
public class ClusterMessage implements RCmbMessage {
    public int type;
    public int port;
    public String ip;
    public int flags;
    public int count;
    public int length;
    public int version;
    public String name;
    public int busPort;
    public long offset;
    public String master;
    public String signature;
    public long configEpoch;
    public long currentEpoch;
    public ClusterState state;
    public byte[] reserved = new byte[34];
    public byte[] messageFlags = new byte[3];
    public byte[] slots = new byte[CLUSTER_SLOTS_BYTES];
    public ClusterMessageData data = new ClusterMessageData();

    @Override
    public String toString() {
        return "ClusterMessage{" +
                "signature='" + signature + '\'' +
                ", length=" + length +
                ", version=" + version +
                ", port=" + port +
                ", type=" + type +
                ", count=" + count +
                ", currentEpoch=" + currentEpoch +
                ", configEpoch=" + configEpoch +
                ", offset=" + offset +
                ", name='" + name + '\'' +
                ", master='" + master + '\'' +
                ", ip='" + ip + '\'' +
                ", busPort=" + busPort +
                ", flags=" + flags +
                ", state=" + state +
                ", messageFlags=" + Arrays.toString(messageFlags) +
                ", data=" + data +
                '}';
    }
}

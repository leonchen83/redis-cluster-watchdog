package com.moilioncircle.replicator.cluster.gossip;

import com.moilioncircle.replicator.cluster.ClusterNode;
import com.moilioncircle.replicator.cluster.Server;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import static com.moilioncircle.replicator.cluster.ClusterConstants.*;
import static com.moilioncircle.replicator.cluster.util.CRC16.crc16;

/**
 * Created by Baoyi Chen on 2017/7/12.
 */
public class ClusterSlotManger {
    private static final Log logger = LogFactory.getLog(ClusterSlotManger.class);
    private Server server;
    private ThinGossip gossip;

    public ClusterSlotManger(ThinGossip gossip) {
        this.gossip = gossip;
        this.server = gossip.server;
    }

    public static boolean bitmapTestBit(byte[] bitmap, int pos) {
        int offset = pos / 8;
        int bit = pos & 7;
        return (bitmap[offset] & (1 << bit)) != 0;
    }

    public static void bitmapSetBit(byte[] bitmap, int pos) {
        int offset = pos / 8;
        int bit = pos & 7;
        bitmap[offset] |= 1 << bit;
    }

    public static void bitmapClearBit(byte[] bitmap, int pos) {
        int offset = pos / 8;
        int bit = pos & 7;
        bitmap[offset] &= ~(1 << bit);
    }

    public boolean clusterMastersHaveSlaves() {
        int slaves = 0;
        for (ClusterNode node : server.cluster.nodes.values()) {
            if (nodeIsSlave(node)) continue;
            slaves += node.numslaves;
        }
        return slaves != 0;
    }

    public boolean clusterNodeSetSlotBit(ClusterNode n, int slot) {
        boolean old = bitmapTestBit(n.slots, slot);
        bitmapSetBit(n.slots, slot);
        if (!old) {
            n.numslots++;
            if (n.numslots == 1 && clusterMastersHaveSlaves())
                n.flags |= CLUSTER_NODE_MIGRATE_TO;
        }
        return old;
    }

    public boolean clusterNodeClearSlotBit(ClusterNode n, int slot) {
        boolean old = bitmapTestBit(n.slots, slot);
        bitmapClearBit(n.slots, slot);
        if (old) n.numslots--;
        return old;
    }

    public boolean clusterNodeGetSlotBit(ClusterNode n, int slot) {
        return bitmapTestBit(n.slots, slot);
    }

    public boolean clusterAddSlot(ClusterNode n, int slot) {
        if (server.cluster.slots[slot] != null) return false;
        clusterNodeSetSlotBit(n, slot);
        server.cluster.slots[slot] = n;
        return true;
    }

    public boolean clusterDelSlot(int slot) {
        ClusterNode n = server.cluster.slots[slot];
        if (n == null) return false;
        clusterNodeClearSlotBit(n, slot);
        server.cluster.slots[slot] = null;
        return true;
    }

    public int clusterDelNodeSlots(ClusterNode node) {
        int deleted = 0;
        for (int j = 0; j < CLUSTER_SLOTS; j++) {
            if (clusterNodeGetSlotBit(node, j)) clusterDelSlot(j);
            deleted++;
        }
        return deleted;
    }

    public static int keyHashSlot(String key) {
        byte[] bytes = key.getBytes();
        int st = key.indexOf('{');
        if (st < 0) return crc16(bytes);
        int ed = key.indexOf('}');
        if (ed < 0 || ed == st + 1) return crc16(bytes); //{}
        if (st > ed) return crc16(bytes); //}{
        return crc16(key.substring(st + 1, ed).getBytes());
    }
}

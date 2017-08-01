package com.moilioncircle.redis.cluster.watchdog.manager;

import com.moilioncircle.redis.cluster.watchdog.state.ClusterNode;
import com.moilioncircle.redis.cluster.watchdog.state.ServerState;

import static com.moilioncircle.redis.cluster.watchdog.ClusterConstants.CLUSTER_NODE_MIGRATE_TO;
import static com.moilioncircle.redis.cluster.watchdog.ClusterConstants.CLUSTER_SLOTS;
import static com.moilioncircle.redis.cluster.watchdog.state.NodeStates.nodeIsSlave;
import static com.moilioncircle.redis.cluster.watchdog.util.CRC16.crc16;

/**
 * @author Leon Chen
 * @since 1.0.0
 */
public class ClusterSlotManger {
    private ServerState server;

    public ClusterSlotManger(ClusterManagers managers) {
        this.server = managers.server;
    }

    public static void bitmapSetBit(byte[] bitmap, int pos) {
        bitmap[pos / 8] |= 1 << (pos & 7);
    }

    public static void bitmapClearBit(byte[] bitmap, int pos) {
        bitmap[pos / 8] &= ~(1 << (pos & 7));
    }

    public static boolean bitmapTestBit(byte[] bitmap, int pos) {
        return (bitmap[pos / 8] & (1 << (pos & 7))) != 0;
    }

    public boolean clusterMastersHaveSlaves() {
        int slaves = 0;
        for (ClusterNode node : server.cluster.nodes.values()) {
            if (nodeIsSlave(node)) continue; slaves += node.slaves.size();
        }
        return slaves != 0;
    }

    public boolean clusterNodeSetSlotBit(ClusterNode node, int slot) {
        boolean r = bitmapTestBit(node.slots, slot);
        bitmapSetBit(node.slots, slot); if (r) return true;
        if (++node.assignedSlots == 1 && clusterMastersHaveSlaves())
            node.flags |= CLUSTER_NODE_MIGRATE_TO;
        return false;
    }

    public boolean clusterNodeClearSlotBit(ClusterNode node, int slot) {
        boolean r = bitmapTestBit(node.slots, slot);
        bitmapClearBit(node.slots, slot); if (r) node.assignedSlots--; return r;
    }

    public boolean clusterAddSlot(ClusterNode node, int slot) {
        if (server.cluster.slots[slot] != null) return false;
        clusterNodeSetSlotBit(node, slot); server.cluster.slots[slot] = node; return true;
    }

    public boolean clusterDelSlot(int slot) {
        ClusterNode node = server.cluster.slots[slot];
        if (node == null) return false; clusterNodeClearSlotBit(node, slot);
        server.cluster.slots[slot] = null; return true;
    }

    public int clusterDelNodeSlots(ClusterNode node) {
        int deleted = 0;
        for (int i = 0; i < CLUSTER_SLOTS; i++) {
            if (bitmapTestBit(node.slots, i)) { clusterDelSlot(i); deleted++; }
        }
        return deleted;
    }

    public void clusterCloseAllSlots() {
        server.cluster.migrating = new ClusterNode[CLUSTER_SLOTS];
        server.cluster.importing = new ClusterNode[CLUSTER_SLOTS];
    }

    public static int keyHashSlot(byte[] key) {
        if (key == null) return 0;
        int st = -1, ed = -1;
        for (int i = 0, len = key.length; i < len; i++) {
            if (key[i] == '{' && st == -1) st = i;
            if (key[i] == '}' && st >= 0) {
                ed = i; break;
            }
        }
        if (st >= 0 && ed >= 0 && ed > st + 1)
            return crc16(key, st + 1, ed) & (CLUSTER_SLOTS - 1);
        return crc16(key) & (CLUSTER_SLOTS - 1);
    }
}

package com.moilioncircle.redis.cluster.watchdog.manager;

import com.moilioncircle.redis.cluster.watchdog.state.ClusterNode;
import com.moilioncircle.redis.cluster.watchdog.state.ClusterNodeFailReport;
import com.moilioncircle.redis.cluster.watchdog.state.ServerState;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;

import static com.moilioncircle.redis.cluster.watchdog.ClusterConstants.*;
import static com.moilioncircle.redis.cluster.watchdog.state.NodeStates.*;
import static java.lang.Math.max;
import static java.util.Comparator.comparingLong;

/**
 * @author Leon Chen
 * @since 1.0.0
 */
public class ClusterNodeManager {
    private static final Log logger = LogFactory.getLog(ClusterConfigManager.class);
    private ServerState server;
    private ClusterManagers managers;

    public ClusterNodeManager(ClusterManagers managers) {
        this.managers = managers;
        this.server = managers.server;
    }

    public void freeClusterNode(ClusterNode node) {
        if (nodeIsSlave(node) && node.master != null) clusterNodeRemoveSlave(node.master, node);
        server.cluster.nodes.remove(node.name);
        if (node.link != null) managers.connections.freeClusterLink(node.link);
    }

    public boolean clusterAddNode(ClusterNode node) {
        return server.cluster.nodes.put(node.name, node) == null;
    }

    public void clusterDelNode(ClusterNode node) {
        for (int i = 0; i < CLUSTER_SLOTS; i++) {
            if (server.cluster.importingSlotsFrom[i] != null && server.cluster.importingSlotsFrom[i].equals(node))
                server.cluster.importingSlotsFrom[i] = null;
            if (server.cluster.migratingSlotsTo[i] != null && server.cluster.migratingSlotsTo[i].equals(node))
                server.cluster.migratingSlotsTo[i] = null;
            if (server.cluster.slots[i] != null && server.cluster.slots[i].equals(node))
                managers.slots.clusterDelSlot(i);
        }

        server.cluster.nodes.values().stream().
                filter(e -> !e.equals(node)).
                forEach(e -> clusterNodeDelFailureReport(e, node));
        freeClusterNode(node);
    }

    public ClusterNode clusterLookupNode(String name) {
        return server.cluster.nodes.get(name);
    }

    public void clusterRenameNode(ClusterNode node, String name) {
        logger.info("Renaming node " + node.name + " into " + name);
        server.cluster.nodes.remove(node.name);
        node.name = name;
        clusterAddNode(node);
    }

    public ClusterNode createClusterNode(String name, int flags) {
        ClusterNode node = new ClusterNode();
        node.name = name == null ? getRandomHexChars() : name;
        node.port = 0;
        node.ip = null;
        node.offset = 0;
        node.link = null;
        node.busPort = 0;
        node.flags = flags;
        node.master = null;
        node.pingTime = 0;
        node.pongTime = 0;
        node.failTime = 0;
        node.configEpoch = 0;
        node.isolatedTime = 0;
        node.assignedSlots = 0;
        node.slaves = new ArrayList<>();
        node.failReports = new ArrayList<>();
        return node;
    }

    public boolean clusterNodeAddFailureReport(ClusterNode failing, ClusterNode sender) {
        for (ClusterNodeFailReport report : failing.failReports) {
            if (!report.node.equals(sender)) continue;
            report.createTime = System.currentTimeMillis();
            return false;
        }
        failing.failReports.add(new ClusterNodeFailReport(sender));
        return true;
    }

    public void clusterNodeCleanupFailureReports(ClusterNode node) {
        List<ClusterNodeFailReport> reports = node.failReports;
        long max = managers.configuration.getClusterNodeTimeout() * CLUSTER_FAIL_REPORT_VALIDITY_MULTI;
        long now = System.currentTimeMillis();
        reports.removeIf(e -> now - e.createTime > max);
    }

    public boolean clusterNodeDelFailureReport(ClusterNode node, ClusterNode sender) {
        Optional<ClusterNodeFailReport> report = node.failReports.stream().filter(e -> e.node.equals(sender)).findFirst();
        if (!report.isPresent()) return false;
        node.failReports.remove(report.get());
        clusterNodeCleanupFailureReports(node);
        return true;
    }

    public int clusterNodeFailureReportsCount(ClusterNode node) {
        clusterNodeCleanupFailureReports(node);
        return node.failReports.size();
    }

    public boolean clusterNodeRemoveSlave(ClusterNode master, ClusterNode slave) {
        boolean rs = master.slaves.remove(slave);
        if (rs && master.slaves.size() == 0) {
            master.flags &= ~CLUSTER_NODE_MIGRATE_TO;
        }
        return rs;
    }

    public boolean clusterNodeAddSlave(ClusterNode master, ClusterNode slave) {
        if (master.slaves.stream().anyMatch(e -> e.equals(slave))) return false;
        master.slaves.add(slave);
        master.flags |= CLUSTER_NODE_MIGRATE_TO;
        return true;
    }

    public int clusterCountNonFailingSlaves(ClusterNode node) {
        return (int) node.slaves.stream().filter(e -> !nodeFailed(e)).count();
    }

    public void clusterSetNodeAsMaster(ClusterNode node) {
        if (nodeIsMaster(node)) return;

        if (node.master != null) {
            clusterNodeRemoveSlave(node.master, node);
            if (node.equals(server.myself)) node.flags |= CLUSTER_NODE_MIGRATE_TO;
        }

        node.flags &= ~CLUSTER_NODE_SLAVE;
        node.flags |= CLUSTER_NODE_MASTER;
        node.master = null;
    }

    public int clusterGetSlaveRank() {
        ClusterNode master = server.myself.master;
        if (master == null) return 0;
        long offset = managers.replications.replicationGetSlaveOffset();
        return (int) master.slaves.stream().filter(e -> e != null && !e.equals(server.myself) && e.offset > offset).count();
    }

    public long clusterGetMaxEpoch() {
        return max(server.cluster.nodes.values().stream().
                max(comparingLong(e -> e.configEpoch)).
                map(e -> e.configEpoch).orElse(0L), server.cluster.currentEpoch);
    }

    public boolean clusterStartHandshake(String ip, int port, int busPort) {
        boolean handshaking = server.cluster.nodes.values().stream().
                anyMatch(e -> nodeInHandshake(e) && e.ip.equalsIgnoreCase(ip) && e.port == port && e.busPort == busPort);
        if (handshaking) return false;

        ClusterNode node = createClusterNode(null, CLUSTER_NODE_HANDSHAKE | CLUSTER_NODE_MEET);
        node.ip = ip;
        node.port = port;
        node.busPort = busPort;
        clusterAddNode(node);
        return true;
    }

    public void clusterSetMyMasterTo(ClusterNode node) {
        if (nodeIsMaster(server.myself)) {
            server.myself.flags &= ~(CLUSTER_NODE_MASTER | CLUSTER_NODE_MIGRATE_TO);
            server.myself.flags |= CLUSTER_NODE_SLAVE;
            managers.slots.clusterCloseAllSlots();
        } else if (server.myself.master != null) {
            clusterNodeRemoveSlave(server.myself.master, server.myself);
        }
        server.myself.master = node;
        clusterNodeAddSlave(node, server.myself);
        managers.replications.replicationSetMaster(node);
    }

    public static String getRandomHexChars() {
        StringBuilder r = new StringBuilder();
        for (int i = 0; i < CLUSTER_NAME_LEN; i++) {
            r.append(HEX_CHARS[ThreadLocalRandom.current().nextInt(HEX_CHARS.length)]);
        }
        return r.toString();
    }
}

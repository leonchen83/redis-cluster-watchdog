package com.moilioncircle.redis.cluster.watchdog.manager;

import com.moilioncircle.redis.cluster.watchdog.ClusterConfiguration;
import com.moilioncircle.redis.cluster.watchdog.state.ClusterNode;
import com.moilioncircle.redis.cluster.watchdog.state.ClusterNodeFailReport;
import com.moilioncircle.redis.cluster.watchdog.state.NodeStates;
import com.moilioncircle.redis.cluster.watchdog.state.ServerState;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.ToLongFunction;

import static com.moilioncircle.redis.cluster.watchdog.ClusterConstants.*;
import static com.moilioncircle.redis.cluster.watchdog.ClusterNodeInfo.valueOf;
import static com.moilioncircle.redis.cluster.watchdog.state.NodeStates.*;
import static java.lang.Math.max;
import static java.util.Comparator.comparingLong;
import static java.util.concurrent.ThreadLocalRandom.current;

/**
 * @author Leon Chen
 * @since 1.0.0
 */
public class ClusterNodeManager {

    private static final Log logger = LogFactory.getLog(ClusterConfigManager.class);

    private ServerState server;
    private ClusterManagers managers;
    private ClusterConfiguration configuration;

    public ClusterNodeManager(ClusterManagers managers) {
        this.managers = managers;
        this.server = managers.server;
        this.configuration = managers.configuration;
    }

    public ClusterNode clusterLookupNode(String name) {
        return server.cluster.nodes.get(name);
    }

    public boolean clusterAddNode(ClusterNode node) {
        return server.cluster.nodes.put(node.name, node) == null;
    }

    public void clusterRenameNode(ClusterNode node, String name) {
        logger.info("Renaming node " + node.name + " into " + name);
        server.cluster.nodes.remove(node.name); node.name = name; clusterAddNode(node);
        managers.notifyNodeAdded(valueOf(node, server.myself));
    }

    public ClusterNode createClusterNode(String name, int flags) {
        ClusterNode n = new ClusterNode();
        n.name = name == null ? getRandomHexChars() : name; n.flags = flags; return n;
    }

    public void clusterDelNode(ClusterNode node) {
        for (int i = 0; i < CLUSTER_SLOTS; i++) {
            if (Objects.equals(server.cluster.slots[i], node)) managers.slots.clusterDelSlot(i);
            if (Objects.equals(server.cluster.migrating[i], node)) server.cluster.migrating[i] = null;
            if (Objects.equals(server.cluster.importing[i], node)) server.cluster.importing[i] = null;
        }
        Predicate<ClusterNode> t = e -> !Objects.equals(e, node);
        Consumer<ClusterNode> c = e -> clusterNodeDelFailureReport(e, node);
        server.cluster.nodes.values().stream().filter(t).forEach(c); freeClusterNode(node);
    }

    /**
     *
     */
    public int clusterNodeFailureReportsCount(ClusterNode node) {
        clusterNodeCleanupFailureReports(node); return node.failReports.size();
    }

    public boolean clusterNodeAddFailureReport(ClusterNode failing, ClusterNode sender) {
        for (ClusterNodeFailReport report : failing.failReports) {
            if (!Objects.equals(report.node, sender)) continue;
            report.createTime = System.currentTimeMillis(); return false;
        }
        failing.failReports.add(new ClusterNodeFailReport(sender)); return true;
    }

    public boolean clusterNodeDelFailureReport(ClusterNode node, ClusterNode sender) {
        Predicate<ClusterNodeFailReport> t = e -> Objects.equals(e.node, sender);
        Optional<ClusterNodeFailReport> r = node.failReports.stream().filter(t).findFirst();
        if (!r.isPresent()) return false; node.failReports.remove(r.get());
        clusterNodeCleanupFailureReports(node); return true;
    }

    public void clusterNodeCleanupFailureReports(ClusterNode node) {
        List<ClusterNodeFailReport> reports = node.failReports;
        long max = configuration.getClusterNodeTimeout() * CLUSTER_FAIL_REPORT_VALIDITY_MULTI;
        long now = System.currentTimeMillis(); reports.removeIf(e -> now - e.createTime > max);
    }

    /**
     *
     */
    public int clusterCountNonFailingSlaves(ClusterNode node) {
        return (int) node.slaves.stream().filter(e -> !nodeFailed(e)).count();
    }

    public boolean clusterNodeAddSlave(ClusterNode master, ClusterNode slave) {
        if (master.slaves.stream().anyMatch(e -> Objects.equals(e, slave))) return false;
        master.slaves.add(slave); master.flags |= CLUSTER_NODE_MIGRATE_TO; return true;
    }

    public int clusterGetSlaveRank() {
        if (server.myself.master == null) return 0;
        long offset = managers.replications.replicationGetSlaveOffset();
        Predicate<ClusterNode> t = e -> Objects.equals(e, server.myself) && e.offset > offset;
        return (int) server.myself.master.slaves.stream().filter(t).count();
    }

    public boolean clusterNodeRemoveSlave(ClusterNode master, ClusterNode slave) {
        boolean r = master.slaves.remove(slave);
        if (r && master.slaves.size() == 0) master.flags &= ~CLUSTER_NODE_MIGRATE_TO; return r;
    }

    public void clusterSetNodeAsMaster(ClusterNode node) {
        if (nodeIsMaster(node)) return;
        if (node.master != null) {
            clusterNodeRemoveSlave(node.master, node);
            if (Objects.equals(node, server.myself)) node.flags |= CLUSTER_NODE_MIGRATE_TO;
        }
        node.flags &= ~CLUSTER_NODE_SLAVE; node.flags |= CLUSTER_NODE_MASTER; node.master = null;
    }

    public long clusterGetMaxEpoch() {
        long epoch = server.cluster.currentEpoch;
        Function<ClusterNode, Long> t1 = e -> e.configEpoch;
        ToLongFunction<ClusterNode> t2 = e -> e.configEpoch;
        return max(server.cluster.nodes.values().stream().max(comparingLong(t2)).map(t1).orElse(0L), epoch);
    }

    public boolean clusterStartHandshake(String ip, int port, int busPort) {
        Predicate<ClusterNode> t = NodeStates::nodeInHandshake;
        t = t.and(e -> e.ip.equalsIgnoreCase(ip) && e.port == port && e.busPort == busPort);
        if (server.cluster.nodes.values().stream().anyMatch(t)) return false; //no handshake.
        ClusterNode node = createClusterNode(null, CLUSTER_NODE_HANDSHAKE | CLUSTER_NODE_MEET);
        node.ip = ip; node.port = port; node.busPort = busPort; clusterAddNode(node); return true;
    }

    public void clusterSetMyMasterTo(ClusterNode node) {
        if (nodeIsMaster(server.myself)) {
            server.myself.flags &= ~(CLUSTER_NODE_MASTER | CLUSTER_NODE_MIGRATE_TO);
            server.myself.flags |= CLUSTER_NODE_SLAVE;
            managers.slots.clusterCloseAllSlots();
        } else if (server.myself.master != null) {
            clusterNodeRemoveSlave(server.myself.master, server.myself);
        }
        clusterNodeAddSlave(server.myself.master = node, server.myself);
        managers.replications.replicationSetMaster(node);
    }

    public static String getRandomHexChars() {
        StringBuilder r = new StringBuilder();
        for (int i = 0; i < CLUSTER_NAME_LEN; i++) r.append(HEX_CHARS[current().nextInt(HEX_CHARS.length)]);
        return r.toString();
    }

    public void freeClusterNode(ClusterNode node) {
        if (nodeIsSlave(node) && node.master != null) clusterNodeRemoveSlave(node.master, node);
        server.cluster.nodes.remove(node.name); if (node.link != null) managers.connections.freeClusterLink(node.link);
    }
}

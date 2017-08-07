package com.moilioncircle.redis.cluster.watchdog;

import com.moilioncircle.redis.cluster.watchdog.codec.ClusterMessageDecoder;
import com.moilioncircle.redis.cluster.watchdog.codec.ClusterMessageEncoder;
import com.moilioncircle.redis.cluster.watchdog.manager.ClusterManagers;
import com.moilioncircle.redis.cluster.watchdog.message.ClusterMessage;
import com.moilioncircle.redis.cluster.watchdog.message.RCmbMessage;
import com.moilioncircle.redis.cluster.watchdog.state.ClusterLink;
import com.moilioncircle.redis.cluster.watchdog.state.ClusterNode;
import com.moilioncircle.redis.cluster.watchdog.state.ClusterState;
import com.moilioncircle.redis.cluster.watchdog.util.net.NioBootstrapImpl;
import com.moilioncircle.redis.cluster.watchdog.util.net.session.DefaultSession;
import com.moilioncircle.redis.cluster.watchdog.util.net.transport.Transport;
import com.moilioncircle.redis.cluster.watchdog.util.net.transport.TransportListener;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BinaryOperator;
import java.util.function.Predicate;

import static com.moilioncircle.redis.cluster.watchdog.ClusterConfigInfo.valueOf;
import static com.moilioncircle.redis.cluster.watchdog.ClusterConstants.*;
import static com.moilioncircle.redis.cluster.watchdog.ClusterState.CLUSTER_FAIL;
import static com.moilioncircle.redis.cluster.watchdog.ClusterState.CLUSTER_OK;
import static com.moilioncircle.redis.cluster.watchdog.state.NodeStates.*;
import static java.lang.Math.max;
import static java.util.concurrent.ThreadLocalRandom.current;

/**
 * @author Leon Chen
 * @since 1.0.0
 */
public class ThinGossip implements Resourcable {
    private static final Log logger = LogFactory.getLog(ThinGossip.class);

    public ClusterManagers managers;
    private ClusterConfiguration configuration;
    private volatile NioBootstrapImpl<RCmbMessage> acceptor;
    private volatile NioBootstrapImpl<RCmbMessage> initiator;

    public ThinGossip(ClusterManagers managers) {
        this.managers = managers;
        this.configuration = managers.configuration;
    }

    @Override
    public void start() {
        this.clusterInit();
        managers.cron.scheduleAtFixedRate(() -> {
            ClusterConfigInfo previous = valueOf(managers.server.cluster);
            clusterCron(); ClusterConfigInfo next = valueOf(managers.server.cluster);
            if (!previous.equals(next)) managers.config.submit(() -> managers.configs.clusterSaveConfig(next));
        }, 0, 100, TimeUnit.MILLISECONDS);
    }

    @Override
    public void stop() {
        stop(0, TimeUnit.MILLISECONDS);
    }

    @Override
    public void stop(long timeout, TimeUnit unit) {
        try {
            NioBootstrapImpl<RCmbMessage> acceptor = this.acceptor;
            if (acceptor != null) acceptor.shutdown().get(timeout, unit);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (ExecutionException e) {
            logger.error("unexpected error", e.getCause());
        } catch (TimeoutException e) {
            logger.error("stop timeout error", e);
        }

        try {
            NioBootstrapImpl<RCmbMessage> initiator = this.initiator;
            if (initiator != null) initiator.shutdown().get(timeout, unit);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (ExecutionException e) {
            logger.error("unexpected error", e.getCause());
        } catch (TimeoutException e) {
            logger.error("stop timeout error", e);
        }
    }

    public void clusterInit() {
        managers.server.cluster = new ClusterState();
        int port = configuration.getClusterAnnouncePort();
        String address = configuration.getClusterAnnounceIp();
        int busPort = configuration.getClusterAnnounceBusPort();

        if (!managers.configs.clusterLoadConfig()) {
            int flags = CLUSTER_NODE_MYSELF | CLUSTER_NODE_MASTER;
            managers.server.myself = managers.nodes.createClusterNode(null, flags);
            //
            String name = managers.server.myself.name;
            managers.server.cluster.myself = managers.server.myself;
            logger.info("No cluster configuration found, I'm " + name);
            managers.nodes.clusterAddNode(this.managers.server.myself);
            managers.notifyNodeAdded(ClusterNodeInfo.valueOf(managers.server.myself));
            ClusterConfigInfo next = ClusterConfigInfo.valueOf(managers.server.cluster);
            managers.config.submit(() -> this.managers.configs.clusterSaveConfig(next));
        }

        acceptor = new NioBootstrapImpl<>();
        acceptor.setEncoder(ClusterMessageEncoder::new);
        acceptor.setDecoder(ClusterMessageDecoder::new); acceptor.setup();
        acceptor.setTransportListener(new AcceptorTransportListener());

        try {
            acceptor.connect(address, busPort).get();
        } catch (InterruptedException | ExecutionException e) {
            if (e instanceof InterruptedException) Thread.currentThread().interrupt();
            else throw new UnsupportedOperationException(e.getCause());
        }

        managers.server.myself.port = port;
        managers.server.myself.busPort = busPort;
    }

    public void clusterCron() {
        try {
            managers.server.iteration++;
            long now = System.currentTimeMillis();
            ClusterNode myself = managers.server.myself;
            long nodeTimeout = configuration.getClusterNodeTimeout();

            String nextAddress = configuration.getClusterAnnounceIp();
            if (!Objects.equals(managers.server.previousAddress, nextAddress)) {
                managers.server.previousAddress = nextAddress;
                if (nextAddress != null) myself.ip = nextAddress; else myself.ip = null;
            }

            managers.server.cluster.pFailNodes = 0;
            long handshakeTimeout = max(nodeTimeout, 1000);
            for (ClusterNode node : new ArrayList<>(managers.server.cluster.nodes.values())) {
                if (nodeIsMyself(node.flags)) continue;
                if (nodeWithoutAddr(node.flags)) continue;
                if (nodePFailed(node.flags)) managers.server.cluster.pFailNodes++;
                if (nodeInHandshake(node) && now - node.createTime > handshakeTimeout) {
                    managers.nodes.clusterDelNode(node); continue;
                }
                if (node.link != null) continue;

                if (initiator == null) {
                    initiator = new NioBootstrapImpl<>(false, configuration.getNetworkConfiguration());
                    //
                    initiator.setEncoder(ClusterMessageEncoder::new);
                    initiator.setDecoder(ClusterMessageDecoder::new); initiator.setup();
                }

                final ClusterLink link = managers.connections.createClusterLink(node);
                TransportListener<RCmbMessage> r = new InitiatorTransportListener(link);
                try {
                    initiator.connect(node.ip, node.busPort).get();
                    initiator.getTransport().setTransportListener(r);
                    link.fd = new DefaultSession<>(initiator.getTransport());
                } catch (InterruptedException | ExecutionException e) {
                    if (e instanceof InterruptedException) Thread.currentThread().interrupt();
                    if (node.pingTime == 0) node.pingTime = System.currentTimeMillis(); continue;
                }
                node.link = link; link.createTime = System.currentTimeMillis();
                long previousPingTime = node.pingTime; boolean meet = nodeInMeet(node.flags);
                managers.messages.clusterSendPing(link, meet ? CLUSTERMSG_TYPE_MEET : CLUSTERMSG_TYPE_PING);
                if (previousPingTime != 0) node.pingTime = previousPingTime; node.flags &= ~CLUSTER_NODE_MEET;
            }

            long minPongTime = 0; ClusterNode minPongNode = null;
            List<ClusterNode> list = new ArrayList<>(managers.server.cluster.nodes.values());

            if (managers.server.iteration % 10 == 0) {
                for (int i = 0; i < 5; i++) {
                    ClusterNode t = list.get(current().nextInt(list.size()));

                    if (nodeIsMyself(t.flags)) continue;
                    if (nodeInHandshake(t.flags)) continue;
                    if (t.link == null || t.pingTime != 0) continue;
                    if (minPongNode == null || minPongTime > t.pongTime) {
                        minPongNode = t; minPongTime = t.pongTime;
                    }
                }
                if (minPongNode != null) {
                    logger.debug("Pinging node " + minPongNode.name);
                    managers.messages.clusterSendPing(minPongNode.link, CLUSTERMSG_TYPE_PING);
                }
            }

            boolean update = false;
            int maxSlaves = 0, mySlaves = 0, isolated = 0;
            for (ClusterNode node : managers.server.cluster.nodes.values()) {
                now = System.currentTimeMillis();

                if (nodeIsMyself(node.flags)) continue;
                if (nodeWithoutAddr(node.flags)) continue;
                if (nodeInHandshake(node.flags)) continue;
                if (nodeIsSlave(myself) && nodeIsMaster(node) && !nodeFailed(node)) {
                    int slaves = managers.nodes.clusterCountNonFailingSlaves(node);
                    if (slaves == 0 && node.assignedSlots > 0 && nodeInMigrate(node.flags)) isolated++;
                    if (slaves > maxSlaves) maxSlaves = slaves;
                    if (Objects.equals(myself.master, node)) mySlaves = slaves;
                }

                if (node.link != null
                        && now - node.link.createTime > nodeTimeout
                        && node.pingTime != 0 && node.pongTime < node.pingTime
                        && now - node.pingTime > nodeTimeout / 2) {

                    managers.connections.freeClusterLink(node.link);
                }

                if (node.link != null && node.pingTime == 0 && (now - node.pongTime) > nodeTimeout / 2) {
                    managers.messages.clusterSendPing(node.link, CLUSTERMSG_TYPE_PING); continue;
                }

                if (node.pingTime == 0) continue;

                if (now - node.pingTime > nodeTimeout && !nodePFailed(node.flags) && !nodeFailed(node.flags)) {
                    logger.debug("*** NODE " + node.name + " possibly failing");
                    node.flags |= CLUSTER_NODE_PFAIL; update = true;
                    managers.notifyNodePFailed(ClusterNodeInfo.valueOf(node, myself));
                }
            }

            if (nodeIsSlave(myself)
                    && managers.server.masterHost == null
                    && myself.master != null
                    && nodeHasAddr(myself.master)) {

                managers.replications.replicationSetMaster(myself.master);
            }

            if (nodeIsSlave(myself)) {
                managers.failovers.clusterHandleSlaveFailover();
                boolean migration = isolated != 0 && maxSlaves >= 2 && mySlaves == maxSlaves;
                if (migration) clusterHandleSlaveMigration(maxSlaves);
            }
            if (update || managers.server.cluster.state == CLUSTER_FAIL) managers.states.clusterUpdateState();
        } catch (Throwable e) { logger.error("unexpected error ", e); }
    }

    public void clusterHandleSlaveMigration(int max) {
        ClusterNode myself = managers.server.myself;
        if (managers.server.myself.master == null) return;
        if (managers.server.cluster.state != CLUSTER_OK) return;
        Predicate<ClusterNode> t = e -> !nodeFailed(e) && !nodePFailed(e);
        int slaves = (int) myself.master.slaves.stream().filter(t).count();
        if (slaves <= this.configuration.getClusterMigrationBarrier()) return;

        ClusterNode target = null;
        long now = System.currentTimeMillis();
        ClusterNode candidate = managers.server.myself;
        for (ClusterNode node : managers.server.cluster.nodes.values()) {
            boolean isolated = true;
            if (nodeFailed(node)) isolated = false;
            if (nodeIsSlave(node)) isolated = false;
            if (!nodeInMigrate(node.flags)) isolated = false;
            if ((slaves = managers.nodes.clusterCountNonFailingSlaves(node)) > 0) isolated = false;

            if (!isolated) node.isolatedTime = 0;
            else {
                if (node.isolatedTime == 0) node.isolatedTime = now;
                if (target == null && node.assignedSlots > 0) target = node;
            }

            if (slaves != max) continue;
            BinaryOperator<ClusterNode> op;
            op = (a, b) -> a.name.compareTo(b.name) >= 0 ? b : a;
            candidate = node.slaves.stream().reduce(myself, op);
        }

        if (target != null && Objects.equals(candidate, myself)
                && (now - target.isolatedTime) > CLUSTER_SLAVE_MIGRATION_DELAY) {
            logger.info("Migrating to orphaned master " + target.name);
            managers.nodes.clusterSetMyMasterTo(target);
        }
    }

    private class InitiatorTransportListener implements TransportListener<RCmbMessage> {

        private final ClusterLink link;
        private InitiatorTransportListener(ClusterLink link) {
            this.link = link;
        }

        @Override
        public void onConnected(Transport<RCmbMessage> t) {
            if (configuration.isVerbose()) logger.info("[initiator] > " + t);
        }

        @Override
        public void onMessage(Transport<RCmbMessage> t, RCmbMessage message) {
            managers.cron.execute(() -> {
                ClusterConfigInfo previous;
                previous = valueOf(managers.server.cluster);
                ClusterMessage hdr = (ClusterMessage) message;
                managers.handlers.get(hdr.type).handle(link, hdr);
                ClusterConfigInfo next = valueOf(managers.server.cluster);
                if (!previous.equals(next)) managers.config.submit(() -> managers.configs.clusterSaveConfig(next));
            });
        }

        @Override
        public void onDisconnected(Transport<RCmbMessage> t, Throwable cause) {
            managers.connections.freeClusterLink(link);
            if (configuration.isVerbose()) logger.info("[initiator] < " + t);
        }
    }

    private class AcceptorTransportListener implements TransportListener<RCmbMessage> {
        @Override
        public void onConnected(Transport<RCmbMessage> t) {
            if (configuration.isVerbose()) logger.info("[acceptor] > " + t);
            ClusterLink link = managers.connections.createClusterLink(null);
            link.fd = new DefaultSession<>(t); managers.server.cfd.put(t, link);
        }

        @Override
        public void onMessage(Transport<RCmbMessage> t, RCmbMessage message) {
            managers.cron.execute(() -> {
                ClusterConfigInfo previous;
                previous = valueOf(managers.server.cluster);
                ClusterMessage hdr = (ClusterMessage) message;
                ClusterLink link = managers.server.cfd.get(t);
                managers.handlers.get(hdr.type).handle(link, hdr);
                ClusterConfigInfo next = valueOf(managers.server.cluster);
                if (!previous.equals(next)) managers.config.submit(() -> managers.configs.clusterSaveConfig(next));
            });
        }

        @Override
        public void onDisconnected(Transport<RCmbMessage> t, Throwable cause) {
            managers.connections.freeClusterLink(managers.server.cfd.remove(t));
            if (configuration.isVerbose()) logger.info("[acceptor] < " + t);
        }
    }
}

package com.moilioncircle.redis.cluster.watchdog;

import com.moilioncircle.redis.cluster.watchdog.codec.ClusterMessageDecoder;
import com.moilioncircle.redis.cluster.watchdog.codec.ClusterMessageEncoder;
import com.moilioncircle.redis.cluster.watchdog.manager.ClusterManagers;
import com.moilioncircle.redis.cluster.watchdog.message.ClusterMessage;
import com.moilioncircle.redis.cluster.watchdog.message.RCmbMessage;
import com.moilioncircle.redis.cluster.watchdog.state.ClusterLink;
import com.moilioncircle.redis.cluster.watchdog.state.ClusterNode;
import com.moilioncircle.redis.cluster.watchdog.state.ClusterState;
import com.moilioncircle.redis.cluster.watchdog.util.net.NetworkConfiguration;
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
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BinaryOperator;
import java.util.function.Predicate;

import static com.moilioncircle.redis.cluster.watchdog.ClusterConstants.*;
import static com.moilioncircle.redis.cluster.watchdog.ClusterNodeInfo.valueOf;
import static com.moilioncircle.redis.cluster.watchdog.ClusterState.CLUSTER_FAIL;
import static com.moilioncircle.redis.cluster.watchdog.ClusterState.CLUSTER_OK;
import static com.moilioncircle.redis.cluster.watchdog.state.NodeStates.*;

/**
 * @author Leon Chen
 * @since 1.0.0
 */
public class ThinGossip {
    private static final Log logger = LogFactory.getLog(ThinGossip.class);

    public ClusterManagers managers;
    private ClusterConfiguration configuration;
    private volatile NioBootstrapImpl<RCmbMessage> acceptor;
    private volatile NioBootstrapImpl<RCmbMessage> initiator;

    public ThinGossip(ClusterManagers managers) {
        this.managers = managers;
        this.configuration = managers.configuration;
    }

    public void start() {
        this.clusterInit();
        managers.cron.scheduleAtFixedRate(() -> {
            ClusterConfigInfo previous = ClusterConfigInfo.valueOf(managers.server.cluster);
            clusterCron();
            ClusterConfigInfo next = ClusterConfigInfo.valueOf(managers.server.cluster);
            if (!previous.equals(next)) managers.config.submit(() -> managers.configs.clusterSaveConfig(next));
        }, 0, 100, TimeUnit.MILLISECONDS);
    }

    public void stop(long timeout, TimeUnit unit) {
        try {
            managers.cron.shutdown();
            managers.cron.awaitTermination(timeout, unit);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

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

        // if myself is a slave. safe to shutdown replication socket.
        managers.replications.replicationUnsetMaster();

        try {
            managers.config.shutdown();
            managers.config.awaitTermination(timeout, unit);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        try {
            managers.worker.shutdown();
            managers.worker.awaitTermination(timeout, unit);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public void clusterInit() {
        managers.server.cluster = new ClusterState();
        if (!managers.configs.clusterLoadConfig()) {
            managers.server.myself = managers.server.cluster.myself = managers.nodes.createClusterNode(null, CLUSTER_NODE_MYSELF | CLUSTER_NODE_MASTER);
            logger.info("No cluster configuration found, I'm " + managers.server.myself.name);
            managers.nodes.clusterAddNode(managers.server.myself);
            managers.notifyNodeAdded(valueOf(managers.server.myself));
            ClusterConfigInfo next = ClusterConfigInfo.valueOf(managers.server.cluster);
            managers.config.submit(() -> managers.configs.clusterSaveConfig(next));
        }

        acceptor = new NioBootstrapImpl<>(true, NetworkConfiguration.defaultSetting());
        acceptor.setEncoder(ClusterMessageEncoder::new);
        acceptor.setDecoder(ClusterMessageDecoder::new);
        acceptor.setup();
        acceptor.setTransportListener(new TransportListener<RCmbMessage>() {
            @Override
            public void onConnected(Transport<RCmbMessage> transport) {
                if (configuration.isVerbose()) {
                    logger.info("[acceptor] > " + transport);
                }
                ClusterLink link = managers.connections.createClusterLink(null);
                link.fd = new DefaultSession<>(transport);
                managers.server.cfd.put(transport, link);
            }

            @Override
            public void onMessage(Transport<RCmbMessage> transport, RCmbMessage message) {
                managers.cron.execute(() -> {
                    ClusterConfigInfo previous = ClusterConfigInfo.valueOf(managers.server.cluster);
                    ClusterMessage hdr = (ClusterMessage) message;
                    managers.handlers.get(hdr.type).handle(managers.server.cfd.get(transport), hdr);
                    ClusterConfigInfo next = ClusterConfigInfo.valueOf(managers.server.cluster);
                    if (!previous.equals(next)) managers.config.submit(() -> managers.configs.clusterSaveConfig(next));
                });
            }

            @Override
            public void onDisconnected(Transport<RCmbMessage> transport, Throwable cause) {
                ClusterLink link = managers.server.cfd.remove(transport);
                managers.connections.freeClusterLink(link);
                if (configuration.isVerbose()) {
                    logger.info("[acceptor] < " + transport);
                }
            }
        });
        try {
            acceptor.connect(configuration.getClusterAnnounceIp(), configuration.getClusterAnnounceBusPort()).get();
        } catch (InterruptedException | ExecutionException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            } else {
                throw new UnsupportedOperationException(e.getCause());
            }
        }

        managers.server.myself.port = configuration.getClusterAnnouncePort();
        managers.server.myself.busPort = configuration.getClusterAnnounceBusPort();
    }

    public void clusterCron() {
        try {
            long now = System.currentTimeMillis();
            managers.server.iteration++;

            String nextAddress = configuration.getClusterAnnounceIp();
            if (!Objects.equals(managers.server.previousAddress, nextAddress)) {
                managers.server.previousAddress = nextAddress;
                if (nextAddress != null) managers.server.myself.ip = nextAddress;
                else managers.server.myself.ip = null;
            }

            long handshakeTimeout = configuration.getClusterNodeTimeout();
            if (handshakeTimeout < 1000) handshakeTimeout = 1000;
            managers.server.cluster.pFailNodes = 0;
            List<ClusterNode> nodes = new ArrayList<>(managers.server.cluster.nodes.values());
            for (ClusterNode node : nodes) {
                if ((node.flags & (CLUSTER_NODE_MYSELF | CLUSTER_NODE_NOADDR)) != 0) continue;
                if ((node.flags & CLUSTER_NODE_PFAIL) != 0) managers.server.cluster.pFailNodes++;
                if (nodeInHandshake(node) && now - node.createTime > handshakeTimeout) {
                    managers.nodes.clusterDelNode(node); continue;
                }
                if (node.link != null) continue;

                if (initiator == null) {
                    initiator = new NioBootstrapImpl<>(false, configuration.getNetworkConfiguration());
                    initiator.setEncoder(ClusterMessageEncoder::new);
                    initiator.setDecoder(ClusterMessageDecoder::new);
                    initiator.setup();
                }

                final ClusterLink link = managers.connections.createClusterLink(node);
                try {
                    initiator.connect(node.ip, node.busPort).get();
                    initiator.getTransport().setTransportListener(new TransportListener<RCmbMessage>() {
                        @Override
                        public void onConnected(Transport<RCmbMessage> transport) {
                            if (configuration.isVerbose()) logger.info("[initiator] > " + transport);
                        }

                        @Override
                        public void onMessage(Transport<RCmbMessage> transport, RCmbMessage message) {
                            managers.cron.execute(() -> {
                                ClusterConfigInfo previous = ClusterConfigInfo.valueOf(managers.server.cluster);
                                ClusterMessage hdr = (ClusterMessage) message;
                                managers.handlers.get(hdr.type).handle(link, hdr);
                                ClusterConfigInfo next = ClusterConfigInfo.valueOf(managers.server.cluster);
                                if (!previous.equals(next)) managers.config.submit(() -> managers.configs.clusterSaveConfig(next));
                            });
                        }

                        @Override
                        public void onDisconnected(Transport<RCmbMessage> transport, Throwable cause) {
                            managers.connections.freeClusterLink(link);
                            if (configuration.isVerbose()) logger.info("[initiator] < " + transport);
                        }
                    });
                    link.fd = new DefaultSession<>(initiator.getTransport());
                } catch (InterruptedException | ExecutionException e) {
                    if (e instanceof InterruptedException) Thread.currentThread().interrupt();
                    if (node.pingTime == 0) node.pingTime = System.currentTimeMillis(); continue;
                }
                node.link = link;
                link.createTime = System.currentTimeMillis();
                long previousPing = node.pingTime;
                managers.messages.clusterSendPing(link, (node.flags & CLUSTER_NODE_MEET) != 0 ? CLUSTERMSG_TYPE_MEET : CLUSTERMSG_TYPE_PING);
                if (previousPing != 0) node.pingTime = previousPing;
                node.flags &= ~CLUSTER_NODE_MEET;
            }

            long minPongTime = 0;
            ClusterNode minPongNode = null;
            if (managers.server.iteration % 10 == 0) {
                for (int i = 0; i < 5; i++) {
                    List<ClusterNode> list = new ArrayList<>(managers.server.cluster.nodes.values());
                    ClusterNode t = list.get(ThreadLocalRandom.current().nextInt(list.size()));

                    if (t.link == null || t.pingTime != 0) continue;
                    if ((t.flags & (CLUSTER_NODE_MYSELF | CLUSTER_NODE_HANDSHAKE)) != 0) continue;
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

                if ((node.flags & (CLUSTER_NODE_MYSELF | CLUSTER_NODE_NOADDR | CLUSTER_NODE_HANDSHAKE)) != 0) continue;
                if (nodeIsSlave(managers.server.myself) && nodeIsMaster(node) && !nodeFailed(node)) {
                    int slaves = managers.nodes.clusterCountNonFailingSlaves(node);
                    if (slaves == 0 && node.assignedSlots > 0 && (node.flags & CLUSTER_NODE_MIGRATE_TO) != 0) isolated++;
                    if (slaves > maxSlaves) maxSlaves = slaves;
                    if (Objects.equals(managers.server.myself.master, node)) mySlaves = slaves;
                }

                if (node.link != null
                        && now - node.link.createTime > configuration.getClusterNodeTimeout()
                        && node.pingTime != 0 && node.pongTime < node.pingTime
                        && now - node.pingTime > configuration.getClusterNodeTimeout() / 2) {
                    managers.connections.freeClusterLink(node.link);
                }

                if (node.link != null && node.pingTime == 0 && (now - node.pongTime) > configuration.getClusterNodeTimeout() / 2) {
                    managers.messages.clusterSendPing(node.link, CLUSTERMSG_TYPE_PING); continue;
                }

                if (node.pingTime == 0) continue;

                if (now - node.pingTime > configuration.getClusterNodeTimeout() && (node.flags & (CLUSTER_NODE_PFAIL | CLUSTER_NODE_FAIL)) == 0) {
                    logger.debug("*** NODE " + node.name + " possibly failing");
                    node.flags |= CLUSTER_NODE_PFAIL; update = true;
                    managers.notifyNodePFailed(valueOf(node, managers.server.myself));
                }
            }

            ClusterNode myself = managers.server.myself;
            if (nodeIsSlave(myself) && managers.server.masterHost == null && myself.master != null && nodeHasAddr(myself.master)) {
                managers.replications.replicationSetMaster(myself.master);
            }

            if (nodeIsSlave(myself)) {
                managers.failovers.clusterHandleSlaveFailover();
                if (isolated != 0 && maxSlaves >= 2 && mySlaves == maxSlaves) clusterHandleSlaveMigration(maxSlaves);
            }
            if (update || managers.server.cluster.state == CLUSTER_FAIL) managers.states.clusterUpdateState();
        } catch (Throwable e) { logger.error("unexpected error ", e); }
    }

    public void clusterHandleSlaveMigration(int max) {
        if (managers.server.cluster.state != CLUSTER_OK) return;
        if (managers.server.myself.master == null) return;
        Predicate<ClusterNode> t = e -> !nodeFailed(e) && !nodePFailed(e);
        int slaves = (int) managers.server.myself.master.slaves.stream().filter(t).count();
        if (slaves <= configuration.getClusterMigrationBarrier()) return;

        ClusterNode target = null;
        long now = System.currentTimeMillis();
        ClusterNode candidate = managers.server.myself;
        for (ClusterNode node : managers.server.cluster.nodes.values()) {
            slaves = 0; boolean isolated = true;
            if (nodeIsSlave(node) || nodeFailed(node)) isolated = false;
            if ((node.flags & CLUSTER_NODE_MIGRATE_TO) == 0) isolated = false;
            if (nodeIsMaster(node)) slaves = managers.nodes.clusterCountNonFailingSlaves(node);
            if (slaves > 0) isolated = false;

            if (isolated) {
                if (target == null && node.assignedSlots > 0) target = node;
                if (node.isolatedTime == 0) node.isolatedTime = now;
            } else node.isolatedTime = 0;

            if (slaves == max) {
                BinaryOperator<ClusterNode> op = (a, b) -> a.name.compareTo(b.name) >= 0 ? b : a;
                candidate = node.slaves.stream().reduce(managers.server.myself, op);
            }
        }

        if (target != null && Objects.equals(candidate, managers.server.myself) && (now - target.isolatedTime) > CLUSTER_SLAVE_MIGRATION_DELAY) {
            logger.info("Migrating to orphaned master " + target.name);
            managers.nodes.clusterSetMyMasterTo(target);
        }
    }
}

package com.moilioncircle.redis.cluster.watchdog;

import com.moilioncircle.redis.cluster.watchdog.codec.ClusterMessageDecoder;
import com.moilioncircle.redis.cluster.watchdog.codec.ClusterMessageEncoder;
import com.moilioncircle.redis.cluster.watchdog.config.ConfigInfo;
import com.moilioncircle.redis.cluster.watchdog.manager.ClusterManagers;
import com.moilioncircle.redis.cluster.watchdog.message.ClusterMessage;
import com.moilioncircle.redis.cluster.watchdog.message.RCmbMessage;
import com.moilioncircle.redis.cluster.watchdog.state.ClusterLink;
import com.moilioncircle.redis.cluster.watchdog.state.ClusterNode;
import com.moilioncircle.redis.cluster.watchdog.state.ClusterState;
import com.moilioncircle.redis.cluster.watchdog.state.States;
import com.moilioncircle.redis.cluster.watchdog.util.net.NioBootstrapConfiguration;
import com.moilioncircle.redis.cluster.watchdog.util.net.NioBootstrapImpl;
import com.moilioncircle.redis.cluster.watchdog.util.net.session.SessionImpl;
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

/**
 * Created by Baoyi Chen on 2017/7/19.
 */
public class ThinGossip {
    private static final Log logger = LogFactory.getLog(ThinGossip.class);

    public ClusterManagers managers;

    public ThinGossip(ClusterManagers managers) {
        this.managers = managers;
    }

    public void start() {
        this.clusterInit();
        managers.executor.scheduleAtFixedRate(() -> {
            ConfigInfo oldInfo = ConfigInfo.valueOf(managers.server.cluster);
            clusterCron();
            ConfigInfo newInfo = ConfigInfo.valueOf(managers.server.cluster);
            if (!oldInfo.equals(newInfo)) managers.file.submit(() -> managers.configs.clusterSaveConfig(newInfo));
        }, 0, 100, TimeUnit.MILLISECONDS);
    }

    public void clusterInit() {
        managers.server.cluster = new ClusterState();
        if (!managers.configs.clusterLoadConfig(managers.configuration.getClusterConfigFile())) {
            managers.server.myself = managers.server.cluster.myself = managers.nodes.createClusterNode(null, ClusterConstants.CLUSTER_NODE_MYSELF | ClusterConstants.CLUSTER_NODE_MASTER);
            logger.info("No cluster configuration found, I'm " + managers.server.myself.name);
            managers.nodes.clusterAddNode(managers.server.myself);
            ConfigInfo info = ConfigInfo.valueOf(managers.server.cluster);
            managers.file.submit(() -> managers.configs.clusterSaveConfig(info));
        }

        NioBootstrapImpl<RCmbMessage> cfd = new NioBootstrapImpl<>(true, new NioBootstrapConfiguration());
        cfd.setEncoder(ClusterMessageEncoder::new);
        cfd.setDecoder(ClusterMessageDecoder::new);
        cfd.setup();
        cfd.setTransportListener(new TransportListener<RCmbMessage>() {
            @Override
            public void onConnected(Transport<RCmbMessage> transport) {
                logger.info("[acceptor] > " + transport.toString());
                ClusterLink link = managers.connections.createClusterLink(null);
                link.fd = new SessionImpl<>(transport);
                managers.server.cfd.put(transport, link);
            }

            @Override
            public void onMessage(Transport<RCmbMessage> transport, RCmbMessage message) {
                managers.executor.execute(() -> {
                    ConfigInfo oldInfo = ConfigInfo.valueOf(managers.server.cluster);
                    clusterProcessPacket(managers.server.cfd.get(transport), (ClusterMessage) message);
                    ConfigInfo newInfo = ConfigInfo.valueOf(managers.server.cluster);
                    if (!oldInfo.equals(newInfo))
                        managers.file.submit(() -> managers.configs.clusterSaveConfig(newInfo));
                });
            }

            @Override
            public void onDisconnected(Transport<RCmbMessage> transport, Throwable cause) {
                logger.info("[acceptor] < " + transport.toString());
                ClusterLink link = managers.server.cfd.remove(transport);
                managers.connections.freeClusterLink(link);
            }
        });
        try {
            cfd.connect(null, managers.configuration.getClusterAnnounceBusPort()).get();
        } catch (InterruptedException | ExecutionException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            } else {
                throw new UnsupportedOperationException(e.getCause());
            }
        }

        managers.server.myself.port = managers.configuration.getClusterAnnouncePort();
        managers.server.myself.cport = managers.configuration.getClusterAnnounceBusPort();
    }

    public void clusterCron() {
        try {
            long now = System.currentTimeMillis();
            managers.server.iteration++;

            String currIp = managers.configuration.getClusterAnnounceIp();
            if (!Objects.equals(managers.server.prevIp, currIp)) {
                managers.server.prevIp = currIp;
                if (currIp != null) managers.server.myself.ip = currIp;
                else managers.server.myself.ip = null;
            }

            long handshakeTimeout = managers.configuration.getClusterNodeTimeout();
            if (handshakeTimeout < 1000) handshakeTimeout = 1000;
            managers.server.cluster.statsPfailNodes = 0;
            List<ClusterNode> nodes = new ArrayList<>(managers.server.cluster.nodes.values());
            for (ClusterNode node : nodes) {
                if ((node.flags & (ClusterConstants.CLUSTER_NODE_MYSELF | ClusterConstants.CLUSTER_NODE_NOADDR)) != 0)
                    continue;

                if ((node.flags & ClusterConstants.CLUSTER_NODE_PFAIL) != 0)
                    managers.server.cluster.statsPfailNodes++;

                if (States.nodeInHandshake(node) && now - node.ctime > handshakeTimeout) {
                    managers.nodes.clusterDelNode(node);
                    continue;
                }

                if (node.link == null) {
                    final ClusterLink link = managers.connections.createClusterLink(node);
                    NioBootstrapImpl<RCmbMessage> fd = new NioBootstrapImpl<>(false, new NioBootstrapConfiguration());
                    fd.setEncoder(ClusterMessageEncoder::new);
                    fd.setDecoder(ClusterMessageDecoder::new);
                    fd.setup();
                    fd.setTransportListener(new TransportListener<RCmbMessage>() {
                        @Override
                        public void onConnected(Transport<RCmbMessage> transport) {
                            logger.info("[initiator] > " + transport.toString());
                        }

                        @Override
                        public void onMessage(Transport<RCmbMessage> transport, RCmbMessage message) {
                            managers.executor.execute(() -> {
                                ConfigInfo oldInfo = ConfigInfo.valueOf(managers.server.cluster);
                                clusterProcessPacket(link, (ClusterMessage) message);
                                ConfigInfo newInfo = ConfigInfo.valueOf(managers.server.cluster);
                                if (!oldInfo.equals(newInfo))
                                    managers.file.submit(() -> managers.configs.clusterSaveConfig(newInfo));
                            });
                        }

                        @Override
                        public void onDisconnected(Transport<RCmbMessage> transport, Throwable cause) {
                            logger.info("[initiator] < " + transport.toString());
                            managers.connections.freeClusterLink(link);
                            fd.shutdown();
                        }
                    });
                    try {
                        fd.connect(node.ip, node.cport).get();
                        link.fd = new SessionImpl<>(fd.getTransport());
                    } catch (InterruptedException | ExecutionException e) {
                        if (e instanceof InterruptedException) {
                            Thread.currentThread().interrupt();
                        }
                        if (node.pingSent == 0) node.pingSent = System.currentTimeMillis();
                        fd.shutdown();
                        continue;
                    }
                    node.link = link;
                    link.ctime = System.currentTimeMillis();
                    long oldPingSent = node.pingSent;
                    managers.messages.clusterSendPing(link, (node.flags & ClusterConstants.CLUSTER_NODE_MEET) != 0 ? ClusterConstants.CLUSTERMSG_TYPE_MEET : ClusterConstants.CLUSTERMSG_TYPE_PING);
                    if (oldPingSent != 0) node.pingSent = oldPingSent;
                    node.flags &= ~ClusterConstants.CLUSTER_NODE_MEET;
                }
            }

            long minPong = 0;
            ClusterNode minPongNode = null;
            if (managers.server.iteration % 10 == 0) {
                for (int i = 0; i < 5; i++) {
                    List<ClusterNode> list = new ArrayList<>(managers.server.cluster.nodes.values());
                    ClusterNode t = list.get(ThreadLocalRandom.current().nextInt(list.size()));

                    if (t.link == null || t.pingSent != 0) continue;
                    if ((t.flags & (ClusterConstants.CLUSTER_NODE_MYSELF | ClusterConstants.CLUSTER_NODE_HANDSHAKE)) != 0)
                        continue;
                    if (minPongNode == null || minPong > t.pongReceived) {
                        minPongNode = t;
                        minPong = t.pongReceived;
                    }
                }
                if (minPongNode != null) {
                    logger.debug("Pinging node " + minPongNode.name);
                    managers.messages.clusterSendPing(minPongNode.link, ClusterConstants.CLUSTERMSG_TYPE_PING);
                }
            }

            boolean update = false;
            int maxSlaves = 0, thisSlaves = 0, orphanedMasters = 0;
            for (ClusterNode node : managers.server.cluster.nodes.values()) {
                now = System.currentTimeMillis();

                if ((node.flags & (ClusterConstants.CLUSTER_NODE_MYSELF | ClusterConstants.CLUSTER_NODE_NOADDR | ClusterConstants.CLUSTER_NODE_HANDSHAKE)) != 0)
                    continue;

                if (States.nodeIsSlave(managers.server.myself) && States.nodeIsMaster(node) && !States.nodeFailed(node)) {
                    int slaves = managers.nodes.clusterCountNonFailingSlaves(node);

                    if (slaves == 0 && node.numslots > 0 && (node.flags & ClusterConstants.CLUSTER_NODE_MIGRATE_TO) != 0) {
                        orphanedMasters++;
                    }
                    if (slaves > maxSlaves) maxSlaves = slaves;
                    if (managers.server.myself.slaveof.equals(node))
                        thisSlaves = slaves;
                }

                if (node.link != null
                        && now - node.link.ctime > managers.configuration.getClusterNodeTimeout()
                        && node.pingSent != 0 && node.pongReceived < node.pingSent
                        && now - node.pingSent > managers.configuration.getClusterNodeTimeout() / 2) {
                    managers.connections.freeClusterLink(node.link);
                }

                if (node.link != null && node.pingSent == 0 && (now - node.pongReceived) > managers.configuration.getClusterNodeTimeout() / 2) {
                    managers.messages.clusterSendPing(node.link, ClusterConstants.CLUSTERMSG_TYPE_PING);
                    continue;
                }

                if (node.pingSent == 0) continue;

                if (now - node.pingSent > managers.configuration.getClusterNodeTimeout() && (node.flags & (ClusterConstants.CLUSTER_NODE_PFAIL | ClusterConstants.CLUSTER_NODE_FAIL)) == 0) {
                    logger.debug("*** NODE " + node.name + " possibly failing");
                    node.flags |= ClusterConstants.CLUSTER_NODE_PFAIL;
                    update = true;
                }
            }

            if (States.nodeIsSlave(managers.server.myself) && managers.server.masterHost == null && managers.server.myself.slaveof != null && States.nodeHasAddr(managers.server.myself.slaveof)) {
                managers.replications.replicationSetMaster(managers.server.myself.slaveof);
            }

            if (States.nodeIsSlave(managers.server.myself) && orphanedMasters != 0 && maxSlaves >= 2 && thisSlaves == maxSlaves) {
                clusterHandleSlaveMigration(maxSlaves);
            }

            if (update || managers.server.cluster.state == ClusterConstants.CLUSTER_FAIL)
                managers.states.clusterUpdateState();
        } catch (Throwable e) {
            logger.error("unexpected error ", e);
        }
    }

    public void clusterHandleSlaveMigration(int maxSlaves) {
        if (managers.server.cluster.state != ClusterConstants.CLUSTER_OK) return;
        if (managers.server.myself.slaveof == null) return;
        int slaves = (int) managers.server.myself.slaveof.slaves.stream().
                filter(x -> !States.nodeFailed(x) && !States.nodePFailed(x)).count();
        if (slaves <= managers.configuration.getClusterMigrationBarrier()) return;

        ClusterNode candidate = managers.server.myself;
        ClusterNode target = null;
        for (ClusterNode node : managers.server.cluster.nodes.values()) {
            slaves = 0;
            boolean isOrphaned = true;

            if (States.nodeIsSlave(node) || States.nodeFailed(node)) isOrphaned = false;
            if ((node.flags & ClusterConstants.CLUSTER_NODE_MIGRATE_TO) == 0) isOrphaned = false;

            if (States.nodeIsMaster(node)) slaves = managers.nodes.clusterCountNonFailingSlaves(node);
            if (slaves > 0) isOrphaned = false;

            if (isOrphaned) {
                if (target == null && node.numslots > 0) target = node;
                if (node.orphanedTime == 0) node.orphanedTime = System.currentTimeMillis();
            } else {
                node.orphanedTime = 0;
            }

            if (slaves == maxSlaves) {
                candidate = node.slaves.stream().reduce(managers.server.myself, (a, b) -> a.name.compareTo(b.name) >= 0 ? b : a);
            }
        }

        if (target != null && candidate.equals(managers.server.myself) && (System.currentTimeMillis() - target.orphanedTime) > ClusterConstants.CLUSTER_SLAVE_MIGRATION_DELAY) {
            logger.info("Migrating to orphaned master " + target.name);
            managers.nodes.clusterSetMyMaster(target);
        }
    }

    public boolean clusterProcessPacket(ClusterLink link, ClusterMessage hdr) {
        return managers.handlers.get(hdr.type).handle(link, hdr);
    }
}

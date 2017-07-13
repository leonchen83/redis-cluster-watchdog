/*
 * Copyright 2016 leon chen
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.moilioncircle.replicator.cluster.gossip;

import com.moilioncircle.replicator.cluster.ClusterLink;
import com.moilioncircle.replicator.cluster.ClusterNode;
import com.moilioncircle.replicator.cluster.ClusterState;
import com.moilioncircle.replicator.cluster.codec.ClusterMsgDecoder;
import com.moilioncircle.replicator.cluster.codec.ClusterMsgEncoder;
import com.moilioncircle.replicator.cluster.message.ClusterMsg;
import com.moilioncircle.replicator.cluster.message.ClusterMsgDataGossip;
import com.moilioncircle.replicator.cluster.message.Message;
import com.moilioncircle.replicator.cluster.message.handler.ClusterMsgHandler;
import com.moilioncircle.replicator.cluster.util.net.NioBootstrapConfiguration;
import com.moilioncircle.replicator.cluster.util.net.NioBootstrapImpl;
import com.moilioncircle.replicator.cluster.util.net.session.SessionImpl;
import com.moilioncircle.replicator.cluster.util.net.transport.Transport;
import com.moilioncircle.replicator.cluster.util.net.transport.TransportListener;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutionException;

import static com.moilioncircle.replicator.cluster.ClusterConstants.*;

/**
 * @author Leon Chen
 * @since 2.1.0
 */
public class ThinGossip1 {
    private static final Log logger = LogFactory.getLog(ThinGossip1.class);

    public ClusterNode myself;

    public Server server = new Server();

    public ClusterConnectionManager connectionManager;
    public ClusterConfigManager configManager;
    public ClusterNodeManager nodeManager;
    public ClusterMsgManager msgManager;
    public ClusterSlotManger slotManger;
    public ReplicationManager replicationManager;
    public ClusterBlacklistManager blacklistManager;
    public ClusterMsgHandlerManager msgHandlerManager;
    public Client client;

    public ThinGossip1() {
        this.connectionManager = new ClusterConnectionManager();
        this.configManager = new ClusterConfigManager(this);
        this.nodeManager = new ClusterNodeManager(this);
        this.msgManager = new ClusterMsgManager(this);
        this.slotManger = new ClusterSlotManger(this);
        this.replicationManager = new ReplicationManager();
        this.blacklistManager = new ClusterBlacklistManager(this);
        this.msgHandlerManager = new ClusterMsgHandlerManager(this);
        this.client = new Client(this);
    }

    public void clusterInit() throws ExecutionException, InterruptedException {
        server.cluster = new ClusterState();
        server.cluster.myself = null;
        server.cluster.currentEpoch = 0;
        server.cluster.state = CLUSTER_FAIL;
        server.cluster.size = 1;
        server.cluster.todoBeforeSleep = 0;
        server.cluster.nodes = new LinkedHashMap<>();
        server.cluster.nodesBlackList = new LinkedHashMap<>();
        for (int i = 0; i < CLUSTERMSG_TYPE_COUNT; i++) {
            server.cluster.statsBusMessagesSent[i] = 0;
            server.cluster.statsBusMessagesReceived[i] = 0;
        }
        server.cluster.statsPfailNodes = 0;

        if (!configManager.clusterLoadConfig(server.clusterConfigfile)) {
            myself = server.cluster.myself = nodeManager.createClusterNode(null, CLUSTER_NODE_MYSELF | CLUSTER_NODE_SLAVE);
            logger.info("No cluster configuration found, I'm " + myself.name);
            nodeManager.clusterAddNode(myself);
            configManager.clusterSaveConfigOrDie();
        }

        NioBootstrapImpl<Message> cfd = new NioBootstrapImpl<>(true, new NioBootstrapConfiguration());
        cfd.setEncoder(ClusterMsgEncoder::new);
        cfd.setDecoder(ClusterMsgDecoder::new);
        cfd.setup();
        cfd.setTransportListener(new TransportListener<Message>() {
            @Override
            public void onConnected(Transport<Message> transport) {
                logger.info("> " + transport.toString());
                server.cfd.add(new SessionImpl<>(transport));
            }

            @Override
            public void onMessage(Transport<Message> transport, Message message) {
                clusterAcceptHandler(transport, message);
            }

            @Override
            public void onException(Transport<Message> transport, Throwable cause) {
                logger.error(cause.getMessage());
            }

            @Override
            public void onDisconnected(Transport<Message> transport, Throwable cause) {
                logger.info("< " + transport.toString());
                //TODO
            }
        });
        cfd.connect(null, server.port).get();

        myself.port = server.port;
        myself.cport = server.port + CLUSTER_PORT_INCR;
        if (server.clusterAnnouncePort != 0) {
            myself.port = server.clusterAnnouncePort;
        }
        if (server.clusterAnnounceBusPort != 0) {
            myself.cport = server.clusterAnnounceBusPort;
        }
    }

    public void clusterAcceptHandler(Transport<Message> transport, Message message) {
        ClusterLink link = connectionManager.createClusterLink(null);
        link.fd = new SessionImpl<>(transport);
        clusterProcessPacket(link, message);
    }

    public long clusterGetMaxEpoch() {
        long max = 0;
        for (ClusterNode node : server.cluster.nodes.values()) {
            if (node.configEpoch > max) max = node.configEpoch;
        }
        if (max < server.cluster.currentEpoch) max = server.cluster.currentEpoch;
        return max;
    }

    public void clusterHandleConfigEpochCollision(ClusterNode sender) {
        if (sender.configEpoch != myself.configEpoch || nodeIsSlave(sender) || nodeIsSlave(myself)) return;
        if (sender.name.compareTo(myself.name) <= 0) return;
        server.cluster.currentEpoch++;
        myself.configEpoch = server.cluster.currentEpoch;
        configManager.clusterSaveConfigOrDie();
        logger.debug("WARNING: configEpoch collision with node " + sender.name + ". configEpoch set to " + myself.configEpoch);
    }

    public void markNodeAsFailingIfNeeded(ClusterNode node) {
        int neededQuorum = server.cluster.size / 2 + 1;

        if (!nodePFailed(node) || nodeFailed(node)) return;

        int failures = nodeManager.clusterNodeFailureReportsCount(node);

        if (nodeIsMaster(myself)) failures++;
        if (failures < neededQuorum) return;

        logger.info("Marking node " + node.name + " as failing (quorum reached).");

        node.flags &= ~CLUSTER_NODE_PFAIL;
        node.flags |= CLUSTER_NODE_FAIL;
        node.failTime = System.currentTimeMillis();

        if (nodeIsMaster(myself)) msgManager.clusterSendFail(node.name);
        clusterDoBeforeSleep(CLUSTER_TODO_UPDATE_STATE | CLUSTER_TODO_SAVE_CONFIG);
    }

    public void clearNodeFailureIfNeeded(ClusterNode node) {
        long now = System.currentTimeMillis();

        if (nodeIsSlave(node) || node.numslots == 0) {
            logger.info("Clear FAIL state for node " + node.name + ": " + (nodeIsSlave(node) ? "slave" : "master without slots") + " is reachable again.");
            node.flags &= ~CLUSTER_NODE_FAIL;
            clusterDoBeforeSleep(CLUSTER_TODO_UPDATE_STATE | CLUSTER_TODO_SAVE_CONFIG);
        }

        if (nodeIsMaster(node) && node.numslots > 0 && now - node.failTime > server.clusterNodeTimeout * CLUSTER_FAIL_UNDO_TIME_MULT) {
            logger.info("Clear FAIL state for node " + node.name + ": is reachable again and nobody is serving its slots after some time.");
            node.flags &= ~CLUSTER_NODE_FAIL;
            clusterDoBeforeSleep(CLUSTER_TODO_UPDATE_STATE | CLUSTER_TODO_SAVE_CONFIG);
        }
    }

    public boolean clusterHandshakeInProgress(String ip, int port, int cport) {
        for (ClusterNode node : server.cluster.nodes.values()) {
            if (nodeInHandshake(node) && node.ip.equalsIgnoreCase(ip) && node.port == port && node.cport == cport)
                return true;
        }
        return false;
    }

    public boolean clusterStartHandshake(String ip, int port, int cport) {
        if (clusterHandshakeInProgress(ip, port, cport)) return false;

        ClusterNode n = nodeManager.createClusterNode(null, CLUSTER_NODE_HANDSHAKE | CLUSTER_NODE_MEET);
        n.ip = ip;
        n.port = port;
        n.cport = cport;
        nodeManager.clusterAddNode(n);
        return true;
    }

    public void clusterProcessGossipSection(ClusterMsg hdr, ClusterLink link) {
        ClusterMsgDataGossip[] gs = hdr.data.gossip;
        ClusterNode sender = link.node != null ? link.node : nodeManager.clusterLookupNode(hdr.sender);
        for (ClusterMsgDataGossip g : gs) {
            int flags = g.flags;
            String ci = configManager.representClusterNodeFlags(flags);
            logger.debug("GOSSIP " + g.nodename + " " + g.ip + ":" + g.port + "@" + g.cport + " " + ci);

            ClusterNode node = nodeManager.clusterLookupNode(g.nodename);

            if (node == null) {
                if (sender != null && (flags & CLUSTER_NODE_NOADDR) == 0 && !blacklistManager.clusterBlacklistExists(g.nodename)) {
                    clusterStartHandshake(g.ip, g.port, g.cport);
                }
                continue;
            }

            if (sender != null && nodeIsMaster(sender) && !node.equals(myself)) {
                if ((flags & (CLUSTER_NODE_FAIL | CLUSTER_NODE_PFAIL)) != 0) {
                    if (nodeManager.clusterNodeAddFailureReport(node, sender)) {
                        logger.debug("Node " + sender.name + " reported node " + node.name + " as not reachable.");
                    }
                    markNodeAsFailingIfNeeded(node);
                } else {
                    if (nodeManager.clusterNodeDelFailureReport(node, sender)) {
                        logger.debug("Node " + sender.name + " reported node " + node.name + " is back online.");
                    }
                }
            }

            if ((flags & (CLUSTER_NODE_FAIL | CLUSTER_NODE_PFAIL)) == 0 && node.pingSent == 0 && nodeManager.clusterNodeFailureReportsCount(node) == 0) {
                //把gossip消息里的节点的pongReceived更新到本地节点上
                //恶心的是这里需要cluster进行ntp同步，而且误差要小于500ms
                long pongtime = g.pongReceived;
                if (pongtime <= (System.currentTimeMillis() + 500) && pongtime > node.pongReceived) {
                    node.pongReceived = pongtime;
                }
            }

            //本地节点有fail状态了, 发送这个gossip消息的节点是正常的,并且本地节点ip端口和远程节点不一致，更新本地节点ip端口信息
            if ((node.flags & (CLUSTER_NODE_FAIL | CLUSTER_NODE_PFAIL)) != 0 && (flags & CLUSTER_NODE_NOADDR) == 0 && (flags & (CLUSTER_NODE_FAIL | CLUSTER_NODE_PFAIL)) == 0 &&
                    (!node.ip.equalsIgnoreCase(g.ip) || node.port != g.port || node.cport != g.cport)) {
                if (node.link != null) connectionManager.freeClusterLink(node.link);
                node.ip = g.ip;
                node.port = g.port;
                node.cport = g.cport;
                node.flags &= ~CLUSTER_NODE_NOADDR;
            }
        }
    }

    public String nodeIp2String(ClusterLink link, String announcedIp) {
        if (announcedIp != null) return announcedIp;
        return link.fd.getRemoteAddress();
    }

    public boolean nodeUpdateAddressIfNeeded(ClusterNode node, ClusterLink link, ClusterMsg hdr) {
        int port = hdr.port;
        int cport = hdr.cport;
        if (link.equals(node.link)) return false;

        String ip = nodeIp2String(link, hdr.myip);

        if (node.port == port && node.cport == cport && ip.equals(node.ip)) return false;

        node.ip = ip;
        node.port = port;
        node.cport = cport;

        //更新ip端口后，把原来的链接释放了
        if (node.link != null) connectionManager.freeClusterLink(node.link);
        logger.warn("Address updated for node " + node.name + ", now " + node.ip + ":" + node.port);

        if (nodeIsSlave(myself) && myself.slaveof.equals(node)) {
            replicationManager.replicationSetMaster(node.ip, node.port);
        }
        return true;
    }

    public void clusterSetNodeAsMaster(ClusterNode n) {
        if (nodeIsMaster(n)) return;

        if (n.slaveof != null) {
            nodeManager.clusterNodeRemoveSlave(n.slaveof, n);
            if (n.equals(myself)) n.flags |= CLUSTER_NODE_MIGRATE_TO;
        }

        n.flags &= ~CLUSTER_NODE_SLAVE;
        n.flags |= CLUSTER_NODE_MASTER;
        n.slaveof = null;

        clusterDoBeforeSleep(CLUSTER_TODO_SAVE_CONFIG | CLUSTER_TODO_UPDATE_STATE);
    }

    public void clusterUpdateSlotsConfigWith(ClusterNode sender, long senderConfigEpoch, byte[] slots) {
        int[] dirtySlots = new int[CLUSTER_SLOTS];
        int dirtySlotsCount = 0;

        ClusterNode newmaster = null;
        ClusterNode curmaster = nodeIsMaster(myself) ? myself : myself.slaveof;
        if (sender.equals(myself)) {
            logger.warn("Discarding UPDATE message about myself.");
            return;
        }

        for (int i = 0; i < CLUSTER_SLOTS; i++) {
            if (slotManger.bitmapTestBit(slots, i)) {
                if (server.cluster.slots[i].equals(sender)) continue;

                if (server.cluster.slots[i] == null || server.cluster.slots[i].configEpoch < senderConfigEpoch) {
                    if (server.cluster.slots[i].equals(myself) && slotManger.countKeysInSlot(i) != 0 && !sender.equals(myself)) {
                        dirtySlots[dirtySlotsCount] = i;
                        dirtySlotsCount++;
                    }

                    if (server.cluster.slots[i].equals(curmaster))
                        newmaster = sender;
                    slotManger.clusterDelSlot(i);
                    slotManger.clusterAddSlot(sender, i);
                    clusterDoBeforeSleep(CLUSTER_TODO_SAVE_CONFIG | CLUSTER_TODO_UPDATE_STATE);
                }
            }
        }

        if (newmaster != null && curmaster.numslots == 0) {
            logger.warn("Configuration change detected. Reconfiguring myself as a replica of " + sender.name);
            clusterSetMaster(sender);
            clusterDoBeforeSleep(CLUSTER_TODO_SAVE_CONFIG | CLUSTER_TODO_UPDATE_STATE);
        } else if (dirtySlotsCount != 0) {
            for (int i = 0; i < dirtySlotsCount; i++)
                slotManger.delKeysInSlot(dirtySlots[i]);
        }
    }

    public boolean clusterProcessPacket(ClusterLink link, Message message) {
        ClusterMsg hdr = (ClusterMsg) message;
        int totlen = hdr.totlen;
        int type = hdr.type;

        if (type < CLUSTERMSG_TYPE_COUNT) {
            server.cluster.statsBusMessagesReceived[type]++;
        }
        logger.debug("--- Processing packet of type " + type + ", " + totlen + " bytes");

        if (hdr.ver != CLUSTER_PROTO_VER) return true;

        ClusterNode sender = nodeManager.clusterLookupNode(hdr.sender);
        if (sender != null && !nodeInHandshake(sender)) {
            if (hdr.currentEpoch > server.cluster.currentEpoch) {
                server.cluster.currentEpoch = hdr.currentEpoch;
            }
            if (hdr.configEpoch > sender.configEpoch) {
                sender.configEpoch = hdr.configEpoch;
                clusterDoBeforeSleep(CLUSTER_TODO_SAVE_CONFIG);
            }
        }

        ClusterMsgHandler handler = msgHandlerManager.get(type);
        if (handler == null) {
            logger.warn("Received unknown packet type: " + type);
            return true;
        } else {
            return handler.handle(sender, link, hdr);
        }
    }

    public void clusterBeforeSleep() {
        if ((server.cluster.todoBeforeSleep & CLUSTER_TODO_UPDATE_STATE) != 0)
            clusterUpdateState();
        if ((server.cluster.todoBeforeSleep & CLUSTER_TODO_SAVE_CONFIG) != 0) {
            configManager.clusterSaveConfigOrDie();
        }
        server.cluster.todoBeforeSleep = 0;
    }

    public void clusterDoBeforeSleep(int flags) {
        server.cluster.todoBeforeSleep |= flags;
    }

    public long amongMinorityTime = 0;
    public long firstCallTime = 0;

    public void clusterUpdateState() {
        server.cluster.todoBeforeSleep &= ~CLUSTER_TODO_UPDATE_STATE;

        if (firstCallTime == 0) firstCallTime = System.currentTimeMillis();
        if (nodeIsMaster(myself) && server.cluster.state == CLUSTER_FAIL && System.currentTimeMillis() - firstCallTime < CLUSTER_WRITABLE_DELAY)
            return;

        int newState = CLUSTER_OK;

        if (server.clusterRequireFullCoverage) {
            for (int i = 0; i < CLUSTER_SLOTS; i++) {
                if (server.cluster.slots[i] == null || (server.cluster.slots[i].flags & CLUSTER_NODE_FAIL) != 0) {
                    newState = CLUSTER_FAIL;
                    break;
                }
            }
        }

        int reachableMasters = 0;
        server.cluster.size = 0;
        for (ClusterNode node : server.cluster.nodes.values()) {
            if (nodeIsMaster(node) && node.numslots > 0) {
                server.cluster.size++;
                if ((node.flags & (CLUSTER_NODE_FAIL | CLUSTER_NODE_PFAIL)) == 0)
                    reachableMasters++;
            }
        }

        int neededQuorum = (server.cluster.size / 2) + 1;

        if (reachableMasters < neededQuorum) {
            newState = CLUSTER_FAIL;
            amongMinorityTime = System.currentTimeMillis();
        }

        if (newState != server.cluster.state) {
            long rejoinDelay = server.clusterNodeTimeout;

            if (rejoinDelay > CLUSTER_MAX_REJOIN_DELAY)
                rejoinDelay = CLUSTER_MAX_REJOIN_DELAY;
            if (rejoinDelay < CLUSTER_MIN_REJOIN_DELAY)
                rejoinDelay = CLUSTER_MIN_REJOIN_DELAY;

            if (newState == CLUSTER_OK && nodeIsMaster(myself) && System.currentTimeMillis() - amongMinorityTime < rejoinDelay) {
                return;
            }

            logger.warn("Cluster state changed: " + (newState == CLUSTER_OK ? "ok" : "fail"));
            server.cluster.state = newState;
        }
    }

    public void clusterSetMaster(ClusterNode n) {
        if (nodeIsMaster(myself)) {
            myself.flags &= ~(CLUSTER_NODE_MASTER | CLUSTER_NODE_MIGRATE_TO);
            myself.flags |= CLUSTER_NODE_SLAVE;
        } else {
            if (myself.slaveof != null)
                nodeManager.clusterNodeRemoveSlave(myself.slaveof, myself);
        }
        myself.slaveof = n;
        nodeManager.clusterNodeAddSlave(n, myself);
        replicationManager.replicationSetMaster(n.ip, n.port);
    }

    public long iteration = 0;
    public String prevIp = null;

    public void clusterCron() {
        long minPong = 0, now = System.currentTimeMillis();
        ClusterNode minPongNode = null;
        iteration++;

        String currIp = server.clusterAnnounceIp;
        boolean changed = false;

        if (prevIp == null && currIp != null) changed = true;
        if (prevIp != null && currIp == null) changed = true;
        if (prevIp != null && currIp != null && !prevIp.equals(currIp)) changed = true;

        if (changed) {
            prevIp = currIp;
            if (currIp != null) {
                myself.ip = currIp;
            } else {
                myself.ip = null;
            }
        }
        long handshakeTimeout = server.clusterNodeTimeout;
        if (handshakeTimeout < 1000) handshakeTimeout = 1000;

        server.cluster.statsPfailNodes = 0;
        for (ClusterNode node : server.cluster.nodes.values()) {
            if ((node.flags & (CLUSTER_NODE_MYSELF | CLUSTER_NODE_NOADDR)) != 0) continue;

            if ((node.flags & CLUSTER_NODE_PFAIL) != 0)
                server.cluster.statsPfailNodes++;

            if (nodeInHandshake(node) && now - node.ctime > handshakeTimeout) {
                nodeManager.clusterDelNode(node);
                continue;
            }

            if (node.link == null) {
                final ClusterLink link = connectionManager.createClusterLink(node);
                NioBootstrapImpl<Message> fd = new NioBootstrapImpl<>(false, new NioBootstrapConfiguration()); //client
                fd.setEncoder(ClusterMsgEncoder::new);
                fd.setDecoder(ClusterMsgDecoder::new);
                fd.setup();
                fd.setTransportListener(new TransportListener<Message>() {
                    @Override
                    public void onConnected(Transport<Message> transport) {
                        link.fd = new SessionImpl<>(transport);
                    }

                    @Override
                    public void onMessage(Transport<Message> transport, Message message) {
                        clusterProcessPacket(link, message);
                    }

                    @Override
                    public void onException(Transport<Message> transport, Throwable cause) {

                    }

                    @Override
                    public void onDisconnected(Transport<Message> transport, Throwable cause) {

                    }
                });
                fd.connect(node.ip, node.cport);

                long oldPingSent = node.pingSent;

                msgManager.clusterSendPing(link, (node.flags & CLUSTER_NODE_MEET) != 0 ? CLUSTERMSG_TYPE_MEET : CLUSTERMSG_TYPE_PING);
                if (oldPingSent != 0) {
                    node.pingSent = oldPingSent;
                }

                node.flags &= ~CLUSTER_NODE_MEET;

                logger.debug("Connecting with Node " + node.name + " at " + node.ip + ":" + node.cport);
            }
        }

        if (iteration % 10 == 0) {
            for (int i = 0; i < 5; i++) {
                List<ClusterNode> list = new ArrayList<>(server.cluster.nodes.values());
                int idx = new Random().nextInt(list.size());
                ClusterNode t = list.get(idx);

                if (t.link == null || t.pingSent != 0) continue;
                if ((t.flags & (CLUSTER_NODE_MYSELF | CLUSTER_NODE_HANDSHAKE)) != 0)
                    continue;
                if (minPongNode == null || minPong > t.pongReceived) {
                    minPongNode = t;
                    minPong = t.pongReceived;
                }
            }
            if (minPongNode != null) {
                logger.debug("Pinging node " + minPongNode.name);
                msgManager.clusterSendPing(minPongNode.link, CLUSTERMSG_TYPE_PING);
            }
        }

        boolean updateState = false;
        int orphanedMasters = 0;
        int maxSlaves = 0;
        int thisSlaves = 0;
        for (ClusterNode node : server.cluster.nodes.values()) {
            now = System.currentTimeMillis();

            if ((node.flags & (CLUSTER_NODE_MYSELF | CLUSTER_NODE_NOADDR | CLUSTER_NODE_HANDSHAKE)) != 0)
                continue;

            if (nodeIsSlave(myself) && nodeIsMaster(node) && !nodeFailed(node)) {
                int okslaves = nodeManager.clusterCountNonFailingSlaves(node);

                if (okslaves == 0 && node.numslots > 0 && (node.flags & CLUSTER_NODE_MIGRATE_TO) != 0) {
                    orphanedMasters++;
                }
                if (okslaves > maxSlaves) maxSlaves = okslaves;
                if (nodeIsSlave(myself) && myself.slaveof.equals(node))
                    thisSlaves = okslaves;
            }

            if (node.link != null && now - node.link.ctime > server.clusterNodeTimeout &&
                    node.pingSent != 0 && node.pongReceived < node.pingSent && now - node.pingSent > server.clusterNodeTimeout / 2) {
                connectionManager.freeClusterLink(node.link);
            }

            if (node.link != null && node.pingSent == 0 && (now - node.pongReceived) > server.clusterNodeTimeout / 2) {
                msgManager.clusterSendPing(node.link, CLUSTERMSG_TYPE_PING);
                continue;
            }

            if (node.pingSent == 0) continue;

            long delay = now - node.pingSent;
            if (delay > server.clusterNodeTimeout) {
                if ((node.flags & (CLUSTER_NODE_PFAIL | CLUSTER_NODE_FAIL)) == 0) {
                    logger.debug("*** NODE " + node.name + " possibly failing");
                    node.flags |= CLUSTER_NODE_PFAIL;
                    updateState = true;
                }
            }
        }

        if (nodeIsSlave(myself) && server.masterhost == null && myself.slaveof != null && nodeHasAddr(myself.slaveof)) {
            replicationManager.replicationSetMaster(myself.slaveof.ip, myself.slaveof.port);
        }

        if (nodeIsSlave(myself) && orphanedMasters != 0 && maxSlaves >= 2 && thisSlaves == maxSlaves) {
            clusterHandleSlaveMigration(maxSlaves);
        }

        if (updateState || server.cluster.state == CLUSTER_FAIL)
            clusterUpdateState();
    }

    public void clusterHandleSlaveMigration(int maxSlaves) {
        int okslaves = 0;
        ClusterNode mymaster = myself.slaveof;

        if (server.cluster.state != CLUSTER_OK) return;

        if (mymaster == null) return;
        for (int i = 0; i < mymaster.numslaves; i++)
            if (!nodeFailed(mymaster.slaves.get(i)) && !nodePFailed(mymaster.slaves.get(i))) okslaves++;
        if (okslaves <= server.clusterMigrationBarrier) return;

        ClusterNode candidate = myself;
        ClusterNode target = null;
        for (ClusterNode node : server.cluster.nodes.values()) {
            okslaves = 0;
            boolean isOrphaned = true;

            if (nodeIsSlave(node) || nodeFailed(node)) isOrphaned = false;
            if ((node.flags & CLUSTER_NODE_MIGRATE_TO) == 0) isOrphaned = false;

            if (nodeIsMaster(node)) okslaves = nodeManager.clusterCountNonFailingSlaves(node);
            if (okslaves > 0) isOrphaned = false;

            if (isOrphaned) {
                if (target == null && node.numslots > 0) target = node;
                if (node.orphanedTime == 0) node.orphanedTime = System.currentTimeMillis();
            } else {
                node.orphanedTime = 0;
            }

            if (okslaves == maxSlaves) {
                for (int i = 0; i < node.numslaves; i++) {
                    if (node.slaves.get(i).name.compareTo(candidate.name) < 0) {
                        candidate = node.slaves.get(i);
                    }
                }
            }
        }

        if (target != null && candidate.equals(myself) && (System.currentTimeMillis() - target.orphanedTime) > CLUSTER_SLAVE_MIGRATION_DELAY) {
            logger.warn("Migrating to orphaned master " + target.name);
            clusterSetMaster(target);
        }
    }
}

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
import com.moilioncircle.replicator.cluster.message.ClusterMsg;
import com.moilioncircle.replicator.cluster.message.ClusterMsgDataGossip;
import com.moilioncircle.replicator.cluster.message.Message;
import com.moilioncircle.replicator.cluster.util.net.NioBootstrapConfiguration;
import com.moilioncircle.replicator.cluster.util.net.NioBootstrapImpl;
import com.moilioncircle.replicator.cluster.util.net.session.SessionImpl;
import com.moilioncircle.replicator.cluster.util.net.transport.Transport;
import com.moilioncircle.replicator.cluster.util.net.transport.TransportListener;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.*;
import java.util.concurrent.ExecutionException;

import static com.moilioncircle.replicator.cluster.ClusterConstants.*;

/**
 * @author Leon Chen
 * @since 2.1.0
 */
public class ThinGossip {
    private static final Log logger = LogFactory.getLog(ThinGossip.class);

    public ClusterNode myself;

    public Server server = new Server();

    public ClusterConnectionManager connectionManager;
    public ClusterConfigManager configManager;
    public ClusterNodeManager nodeManager;
    public ClusterMsgManager msgManager;
    public ClusterSlotManger slotManger;
    public ReplicationManager replicationManager;

    public ThinGossip() {
        this.connectionManager = new ClusterConnectionManager();
        this.configManager = new ClusterConfigManager(this);
        this.nodeManager = new ClusterNodeManager(this);
        this.msgManager = new ClusterMsgManager(this);
        this.slotManger = new ClusterSlotManger(this);
        this.replicationManager = new ReplicationManager();
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
        server.cluster.failoverAuthTime = 0;
        server.cluster.failoverAuthCount = 0;
        server.cluster.failoverAuthRank = 0;
        server.cluster.failoverAuthEpoch = 0;
        server.cluster.cantFailoverReason = CLUSTER_CANT_FAILOVER_NONE;
        server.cluster.lastVoteEpoch = 0;
        for (int i = 0; i < CLUSTERMSG_TYPE_COUNT; i++) {
            server.cluster.statsBusMessagesSent[i] = 0;
            server.cluster.statsBusMessagesReceived[i] = 0;
        }
        server.cluster.statsPfailNodes = 0;
        slotManger.clusterCloseAllSlots();

        boolean saveconf = false;
        if (!configManager.clusterLoadConfig(server.clusterConfigfile)) {
            myself = server.cluster.myself = nodeManager.createClusterNode(null, CLUSTER_NODE_MYSELF | CLUSTER_NODE_MASTER);
            logger.info("No cluster configuration found, I'm " + myself.name);
            nodeManager.clusterAddNode(myself);
            saveconf = true;
        }
        if (saveconf) configManager.clusterSaveConfigOrDie();

        NioBootstrapImpl<Message> cfd = new NioBootstrapImpl<>(true, new NioBootstrapConfiguration());
        // TODO cfd.setEncoder();
        // TODO cfd.setDecoder();
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

        // server.cluster.slotsToKeys = new xx;
        // server.cluster.slotsKeysCount = 0;

        myself.port = server.port;
        myself.cport = server.port + CLUSTER_PORT_INCR;
        if (server.clusterAnnouncePort != 0) {
            myself.port = server.clusterAnnouncePort;
        }
        if (server.clusterAnnounceBusPort != 0) {
            myself.cport = server.clusterAnnounceBusPort;
        }

        server.cluster.mfEnd = 0;
        resetManualFailover();
    }

    void clusterReset(boolean force) {
        if (nodeIsSlave(myself)) {
            clusterSetNodeAsMaster(myself);
            replicationManager.replicationUnsetMaster();
        }

        slotManger.clusterCloseAllSlots();
        resetManualFailover();

        for (int i = 0; i < CLUSTER_SLOTS; i++) slotManger.clusterDelSlot(i);

        for (Map.Entry<String, ClusterNode> entry : server.cluster.nodes.entrySet()) {
            ClusterNode node = entry.getValue();
            if (node.equals(myself)) continue;
            nodeManager.clusterDelNode(node);
        }

        if (force) {
            server.cluster.currentEpoch = 0;
            server.cluster.lastVoteEpoch = 0;
            myself.configEpoch = 0;
            logger.warn("configEpoch set to 0 via CLUSTER RESET HARD");
            String old = myself.name;
            server.cluster.nodes.remove(old);
            myself.name = nodeManager.getRandomHexChars();
            nodeManager.clusterAddNode(myself);
            logger.info("Node hard reset, now I'm " + myself.name);
        }

        clusterDoBeforeSleep(CLUSTER_TODO_SAVE_CONFIG | CLUSTER_TODO_UPDATE_STATE);
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

    public void clusterBlacklistCleanup() {
        Iterator<ClusterNode> it = server.cluster.nodesBlackList.values().iterator();
        while (it.hasNext()) {
            ClusterNode node = it.next();
            long expire = dictGetUnsignedIntegerVal(node);
            if (expire < System.currentTimeMillis()) it.remove();
        }
    }

    private long dictGetUnsignedIntegerVal(ClusterNode node) {
        //TODO
        return 0;
    }

    public void clusterBlacklistAddNode(ClusterNode node) {
        clusterBlacklistCleanup();
        ClusterNode de = server.cluster.nodesBlackList.get(node.name);
        dictSetUnsignedIntegerVal(de, System.currentTimeMillis() + CLUSTER_BLACKLIST_TTL * 1000);
    }

    private void dictSetUnsignedIntegerVal(ClusterNode de, long l) {
        //TODO
    }

    public boolean clusterBlacklistExists(String nodename) {
        clusterBlacklistCleanup();
        return server.cluster.nodesBlackList.containsKey(nodename);
    }

    public void markNodeAsFailingIfNeeded(ClusterNode node) {
        int neededQuorum = server.cluster.size / 2 + 1;

        if (!nodeTimedOut(node) || nodeFailed(node)) return;

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
                if (sender != null && (flags & CLUSTER_NODE_NOADDR) == 0 && !clusterBlacklistExists(g.nodename)) {
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

                if (server.cluster.importingSlotsFrom[i] != null) continue;

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

        int flags = hdr.flags;
        long senderCurrentEpoch = 0, senderConfigEpoch = 0;

        ClusterNode sender = nodeManager.clusterLookupNode(hdr.sender);
        if (sender != null && !nodeInHandshake(sender)) {
            senderCurrentEpoch = hdr.currentEpoch;
            senderConfigEpoch = hdr.configEpoch;
            if (senderCurrentEpoch > server.cluster.currentEpoch) {
                server.cluster.currentEpoch = senderCurrentEpoch;
            }
            if (senderConfigEpoch > sender.configEpoch) {
                sender.configEpoch = senderConfigEpoch;
                clusterDoBeforeSleep(CLUSTER_TODO_SAVE_CONFIG);
            }

            sender.replOffset = hdr.offset;
            sender.replOffsetTime = System.currentTimeMillis();

            if (server.cluster.mfEnd > 0 && nodeIsSlave(myself) && myself.slaveof.equals(sender)
                    && (hdr.mflags[0] & CLUSTERMSG_FLAG0_PAUSED) != 0 && server.cluster.mfMasterOffset == 0) {
                server.cluster.mfMasterOffset = sender.replOffset;
                logger.warn("Received replication offset for paused master manual failover: " + server.cluster.mfMasterOffset);
            }
        }

        if (type == CLUSTERMSG_TYPE_PING || type == CLUSTERMSG_TYPE_MEET) {
            logger.debug("Ping packet received: " + link.node);

            if ((type == CLUSTERMSG_TYPE_MEET || myself.ip == null) && server.clusterAnnounceIp == null) {
                String ip;
                if ((ip = link.fd.getLocalAddress()) != null && !ip.equals(myself.ip)) {
                    myself.ip = ip;
                    logger.warn("IP address for this node updated to " + myself.ip);
                    clusterDoBeforeSleep(CLUSTER_TODO_SAVE_CONFIG);
                }
            }

            if (sender == null && type == CLUSTERMSG_TYPE_MEET) {
                ClusterNode node = nodeManager.createClusterNode(null, CLUSTER_NODE_HANDSHAKE);
                node.ip = nodeIp2String(link, hdr.myip);
                node.port = hdr.port;
                node.cport = hdr.cport;
                nodeManager.clusterAddNode(node);
                clusterDoBeforeSleep(CLUSTER_TODO_SAVE_CONFIG);
            }

            if (sender == null && type == CLUSTERMSG_TYPE_MEET) {
                clusterProcessGossipSection(hdr, link);
            }

            msgManager.clusterSendPing(link, CLUSTERMSG_TYPE_PONG);
        }

        if (type == CLUSTERMSG_TYPE_PING || type == CLUSTERMSG_TYPE_PONG || type == CLUSTERMSG_TYPE_MEET) {
            logger.debug((type == CLUSTERMSG_TYPE_PING ? "ping" : "pong") + " packet received: " + link.node);

            if (link.node != null) {
                if (nodeInHandshake(link.node)) {
                    if (sender != null) {
                        logger.debug("Handshake: we already know node " + sender.name + ", updating the address if needed.");
                        if (nodeUpdateAddressIfNeeded(sender, link, hdr)) {
                            clusterDoBeforeSleep(CLUSTER_TODO_SAVE_CONFIG | CLUSTER_TODO_UPDATE_STATE);
                        }
                        nodeManager.clusterDelNode(link.node);
                        return false;
                    }

                    nodeManager.clusterRenameNode(link.node, hdr.sender);
                    logger.debug("Handshake with node " + link.node.name + " completed.");
                    link.node.flags &= ~CLUSTER_NODE_HANDSHAKE;
                    link.node.flags |= flags & (CLUSTER_NODE_MASTER | CLUSTER_NODE_SLAVE);
                    clusterDoBeforeSleep(CLUSTER_TODO_SAVE_CONFIG);
                } else if (!link.node.name.equals(hdr.sender)) {
                    logger.debug("PONG contains mismatching sender ID. About node " + link.node.name + " added " + (System.currentTimeMillis() - link.node.ctime) + " ms ago, having flags " + link.node.flags);
                    link.node.flags |= CLUSTER_NODE_NOADDR;
                    link.node.ip = null;
                    link.node.port = 0;
                    link.node.cport = 0;
                    connectionManager.freeClusterLink(link);
                    clusterDoBeforeSleep(CLUSTER_TODO_SAVE_CONFIG);
                    return false;
                }
            }

            if (sender != null && type == CLUSTERMSG_TYPE_PING && !nodeInHandshake(sender) && nodeUpdateAddressIfNeeded(sender, link, hdr)) {
                clusterDoBeforeSleep(CLUSTER_TODO_SAVE_CONFIG | CLUSTER_TODO_UPDATE_STATE);
            }

            if (link.node != null && type == CLUSTERMSG_TYPE_PONG) {
                link.node.pongReceived = System.currentTimeMillis();
                link.node.pingSent = 0;

                if (nodeTimedOut(link.node)) { //nodeTimedOut判断是否是CLUSTER_NODE_PFAIL
                    link.node.flags &= ~CLUSTER_NODE_PFAIL;
                    clusterDoBeforeSleep(CLUSTER_TODO_SAVE_CONFIG | CLUSTER_TODO_UPDATE_STATE);
                } else if (nodeFailed(link.node)) {
                    clearNodeFailureIfNeeded(link.node);
                }
            }

            if (sender != null) {
                if (hdr.slaveof.equals(CLUSTER_NODE_NULL_NAME)) { // hdr.slaveof == null
                    clusterSetNodeAsMaster(sender);
                } else {
                    ClusterNode master = nodeManager.clusterLookupNode(hdr.slaveof);

                    if (nodeIsMaster(sender)) {
                        slotManger.clusterDelNodeSlots(sender);
                        sender.flags &= ~(CLUSTER_NODE_MASTER | CLUSTER_NODE_MIGRATE_TO);
                        sender.flags |= CLUSTER_NODE_SLAVE;

                        clusterDoBeforeSleep(CLUSTER_TODO_SAVE_CONFIG | CLUSTER_TODO_UPDATE_STATE);
                    }

                    if (master != null && !sender.slaveof.equals(master)) {
                        if (sender.slaveof != null) nodeManager.clusterNodeRemoveSlave(sender.slaveof, sender);
                        nodeManager.clusterNodeAddSlave(master, sender);
                        sender.slaveof = master;
                        clusterDoBeforeSleep(CLUSTER_TODO_SAVE_CONFIG);
                    }
                }
            }

            ClusterNode senderMaster = null;
            boolean dirtySlots = false;

            if (sender != null) {
                senderMaster = nodeIsMaster(sender) ? sender : sender.slaveof;
                if (senderMaster != null) {
                    dirtySlots = !Arrays.equals(senderMaster.slots, hdr.myslots);
                }
            }

            if (sender != null && nodeIsMaster(sender) && dirtySlots)
                clusterUpdateSlotsConfigWith(sender, senderConfigEpoch, hdr.myslots);

            if (sender != null && dirtySlots) {
                for (int i = 0; i < CLUSTER_SLOTS; i++) {
                    if (slotManger.bitmapTestBit(hdr.myslots, i)) {
                        if (server.cluster.slots[i].equals(sender) || server.cluster.slots[i] == null) continue;
                        if (server.cluster.slots[i].configEpoch > senderConfigEpoch) {
                            logger.debug("Node " + sender.name + " has old slots configuration, sending an UPDATE message about " + server.cluster.slots[i].name);
                            msgManager.clusterSendUpdate(sender.link, server.cluster.slots[i]);
                            break;
                        }
                    }
                }
            }

            if (sender != null && nodeIsMaster(myself) && nodeIsMaster(sender) && senderConfigEpoch == myself.configEpoch) {
                clusterHandleConfigEpochCollision(sender);
            }

            if (sender != null) clusterProcessGossipSection(hdr, link);
        } else if (type == CLUSTERMSG_TYPE_FAIL) {
            if (sender != null) {
                ClusterNode failing = nodeManager.clusterLookupNode(hdr.data.about.nodename);
                if (failing != null && (failing.flags & (CLUSTER_NODE_FAIL | CLUSTER_NODE_MYSELF)) == 0) {
                    logger.info("FAIL message received from " + hdr.sender + " about " + hdr.data.about.nodename);
                    failing.flags |= CLUSTER_NODE_FAIL;
                    failing.failTime = System.currentTimeMillis();
                    failing.flags &= ~CLUSTER_NODE_PFAIL;
                    clusterDoBeforeSleep(CLUSTER_TODO_SAVE_CONFIG | CLUSTER_TODO_UPDATE_STATE);
                }
            } else {
                logger.info("Ignoring FAIL message from unknown node " + hdr.sender + " about " + hdr.data.about.nodename);
            }
        } else if (type == CLUSTERMSG_TYPE_PUBLISH) {
            //NOP unsupported
        } else if (type == CLUSTERMSG_TYPE_FAILOVER_AUTH_REQUEST) {
            if (sender == null) return true;
            msgManager.clusterSendFailoverAuthIfNeeded(sender, hdr);
        } else if (type == CLUSTERMSG_TYPE_FAILOVER_AUTH_ACK) {
            if (sender == null) return true;
            if (nodeIsMaster(sender) && sender.numslots > 0 && senderCurrentEpoch >= server.cluster.failoverAuthEpoch) {
                server.cluster.failoverAuthCount++;
                clusterDoBeforeSleep(CLUSTER_TODO_HANDLE_FAILOVER);
            }
        } else if (type == CLUSTERMSG_TYPE_MFSTART) {
            if (sender == null || !sender.slaveof.equals(myself)) return true;
            resetManualFailover();
            server.cluster.mfEnd = System.currentTimeMillis() + CLUSTER_MF_TIMEOUT;
            server.cluster.mfSlave = sender;
            pauseClients(System.currentTimeMillis() + (CLUSTER_MF_TIMEOUT * 2));
            logger.warn("Manual failover requested by slave " + sender.name);
        } else if (type == CLUSTERMSG_TYPE_UPDATE) {
            long reportedConfigEpoch = hdr.data.nodecfg.configEpoch;

            if (sender == null) return true;
            ClusterNode n = nodeManager.clusterLookupNode(hdr.data.nodecfg.nodename);
            if (n == null) return true;
            if (n.configEpoch >= reportedConfigEpoch) return true;

            if (nodeIsSlave(n)) clusterSetNodeAsMaster(n);

            n.configEpoch = reportedConfigEpoch;
            clusterDoBeforeSleep(CLUSTER_TODO_SAVE_CONFIG);

            clusterUpdateSlotsConfigWith(n, reportedConfigEpoch, hdr.data.nodecfg.slots);
        } else {
            logger.warn("Received unknown packet type: " + type);
        }
        return true;
    }

    public void pauseClients(long l) {
        //TODO
    }

    public static long lastlogTime = 0;

    public void clusterLogCantFailover(int reason) {
        long nologFailTime = server.clusterNodeTimeout + 5000;

        if (reason == server.cluster.cantFailoverReason && System.currentTimeMillis() - lastlogTime < CLUSTER_CANT_FAILOVER_RELOG_PERIOD)
            return;

        server.cluster.cantFailoverReason = reason;

        if (myself.slaveof != null && nodeFailed(myself.slaveof) && (System.currentTimeMillis() - myself.slaveof.failTime) < nologFailTime)
            return;

        String msg;
        switch (reason) {
            case CLUSTER_CANT_FAILOVER_DATA_AGE:
                msg = "Disconnected from master for longer than allowed. Please check the 'cluster-slave-validity-factor' configuration option.";
                break;
            case CLUSTER_CANT_FAILOVER_WAITING_DELAY:
                msg = "Waiting the delay before I can start a new failover.";
                break;
            case CLUSTER_CANT_FAILOVER_EXPIRED:
                msg = "Failover attempt expired.";
                break;
            case CLUSTER_CANT_FAILOVER_WAITING_VOTES:
                msg = "Waiting for votes, but majority still not reached.";
                break;
            default:
                msg = "Unknown reason code.";
                break;
        }
        lastlogTime = System.currentTimeMillis();
        logger.warn("Currently unable to failover: " + msg);
    }

    public void clusterFailoverReplaceYourMaster() {
        ClusterNode oldmaster = myself.slaveof;

        if (nodeIsMaster(myself) || oldmaster == null) return;

        clusterSetNodeAsMaster(myself);
        replicationManager.replicationUnsetMaster();

        for (int i = 0; i < CLUSTER_SLOTS; i++) {
            if (slotManger.clusterNodeGetSlotBit(oldmaster, i)) {
                slotManger.clusterDelSlot(i);
                slotManger.clusterAddSlot(myself, i);
            }
        }

        clusterUpdateState();
        configManager.clusterSaveConfigOrDie();
        msgManager.clusterBroadcastPong(CLUSTER_BROADCAST_ALL);

        resetManualFailover();
    }

    public void clusterHandleSlaveFailover() {
        //此方法要求myself是slave而且myself有master而且非手动模式下master有slot还得是挂的状态
        //还有一种情况是200ms检测一次是否需要failover 在clusterCron里

        long authAge = System.currentTimeMillis() - server.cluster.failoverAuthTime;
        int neededQuorum = (server.cluster.size / 2) + 1;
        boolean manualFailover = server.cluster.mfEnd != 0 && server.cluster.mfCanStart;

        server.cluster.todoBeforeSleep &= ~CLUSTER_TODO_HANDLE_FAILOVER;

        long authTimeout = server.clusterNodeTimeout * 2;
        if (authTimeout < 2000) authTimeout = 2000;
        long authRetryTime = authTimeout * 2;

        /* Pre conditions to run the function, that must be met both in case
         * of an automatic or manual failover:
         * 1) We are a slave.
         * 2) Our master is flagged as FAIL, or this is a manual failover.
         * 3) It is serving slots. */
        if (nodeIsMaster(myself) || myself.slaveof == null || (!nodeFailed(myself.slaveof) && !manualFailover) || myself.slaveof.numslots == 0) {
            server.cluster.cantFailoverReason = CLUSTER_CANT_FAILOVER_NONE;
            return;
        }

        /* Set dataAge to the number of seconds we are disconnected from the master. */
        long dataAge;
        if (server.replState == REPL_STATE_CONNECTED) {
            dataAge = (System.currentTimeMillis() - server.lastinteraction) * 1000;
        } else {
            dataAge = (System.currentTimeMillis() - server.replDownSince) * 1000;
        }

        //减去clusterNodeTimeout时间,因为断开之后要等一个clusterNodeTimeout
        if (dataAge > server.clusterNodeTimeout)
            dataAge -= server.clusterNodeTimeout;

        //超过了要等的最大时间,如果不是手动的话返回
        if (server.clusterSlaveValidityFactor != 0 && dataAge > server.replPingSlavePeriod * 1000 + server.clusterNodeTimeout * server.clusterSlaveValidityFactor) {
            if (!manualFailover) {
                clusterLogCantFailover(CLUSTER_CANT_FAILOVER_DATA_AGE);
                return;
            }
        }

        if (authAge > authRetryTime) {
            server.cluster.failoverAuthTime = System.currentTimeMillis() + 500 + new Random().nextInt(500);
            server.cluster.failoverAuthCount = 0;
            server.cluster.failoverAuthSent = false;
            server.cluster.failoverAuthRank = nodeManager.clusterGetSlaveRank();// 大于myself repl offset的同级的slave有多少个 ,在集群中,这个值在每个node中都不同,所以形成rank
            server.cluster.failoverAuthTime += server.cluster.failoverAuthRank * 1000;
            if (server.cluster.mfEnd != 0) {
                server.cluster.failoverAuthTime = System.currentTimeMillis();
                server.cluster.failoverAuthRank = 0;
            }
            logger.warn("Start of election delayed for " + (server.cluster.failoverAuthTime - System.currentTimeMillis()) + " milliseconds (rank #" + server.cluster.failoverAuthRank + ", offset " + replicationManager.replicationGetSlaveOffset() + ").");
            //向同级的slave广播一次
            msgManager.clusterBroadcastPong(CLUSTER_BROADCAST_LOCAL_SLAVES);
            return;
        }

        //failoverAuthRequest还没发送 自动failover rank有变化的时候更新failoverAuthTime,failoverAuthRank
        if (!server.cluster.failoverAuthSent && server.cluster.mfEnd == 0) {
            int newrank = nodeManager.clusterGetSlaveRank();
            if (newrank > server.cluster.failoverAuthRank) {
                long addedDelay = (newrank - server.cluster.failoverAuthRank) * 1000;
                server.cluster.failoverAuthTime += addedDelay;
                server.cluster.failoverAuthRank = newrank;
                logger.warn("Slave rank updated to #" + newrank + ", added " + addedDelay + " milliseconds of delay.");
            }
        }

        if (System.currentTimeMillis() < server.cluster.failoverAuthTime) {
            clusterLogCantFailover(CLUSTER_CANT_FAILOVER_WAITING_DELAY);
            return;
        }

        if (authAge > authTimeout) {
            clusterLogCantFailover(CLUSTER_CANT_FAILOVER_EXPIRED);
            return;
        }

        //还没发送failover auth request
        if (!server.cluster.failoverAuthSent) {
            server.cluster.currentEpoch++;
            server.cluster.failoverAuthEpoch = server.cluster.currentEpoch;
            logger.warn("Starting a failover election for epoch " + server.cluster.currentEpoch);
            // 发送CLUSTERMSG_TYPE_FAILOVER_AUTH_REQUEST
            msgManager.clusterRequestFailoverAuth();
            server.cluster.failoverAuthSent = true;
            clusterDoBeforeSleep(CLUSTER_TODO_SAVE_CONFIG | CLUSTER_TODO_UPDATE_STATE);
            return;
        }

        if (server.cluster.failoverAuthCount >= neededQuorum) {

            logger.warn("Failover election won: I'm the new master.");

            if (myself.configEpoch < server.cluster.failoverAuthEpoch) {
                myself.configEpoch = server.cluster.failoverAuthEpoch;
                logger.warn("configEpoch set to " + myself.configEpoch + " after successful failover");
            }

            clusterFailoverReplaceYourMaster();
        } else {
            clusterLogCantFailover(CLUSTER_CANT_FAILOVER_WAITING_VOTES);
        }
    }

    public void clusterHandleSlaveMigration(int maxSlaves) {
        int okslaves = 0;
        ClusterNode mymaster = myself.slaveof;

        if (server.cluster.state != CLUSTER_OK) return;

        if (mymaster == null) return;
        for (int i = 0; i < mymaster.numslaves; i++)
            if (!nodeFailed(mymaster.slaves.get(i)) && !nodeTimedOut(mymaster.slaves.get(i))) okslaves++;
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

        if (target != null && candidate.equals(myself) &&
                (System.currentTimeMillis() - target.orphanedTime) > CLUSTER_SLAVE_MIGRATION_DELAY) {
            logger.warn("Migrating to orphaned master " + target.name);
            clusterSetMaster(target);
        }
    }

    public void resetManualFailover() {
        if (server.cluster.mfEnd != 0 && clientsArePaused()) {
            server.clientsPauseEndTime = 0;
            clientsArePaused();
        }
        server.cluster.mfEnd = 0;
        server.cluster.mfCanStart = false;
        server.cluster.mfSlave = null;
        server.cluster.mfMasterOffset = 0;
    }

    private boolean clientsArePaused() {
        //TODO
        return false;
    }

    public void manualFailoverCheckTimeout() {
        if (server.cluster.mfEnd != 0 && server.cluster.mfEnd < System.currentTimeMillis()) {
            logger.warn("Manual failover timed out.");
            resetManualFailover();
        }
    }

    public void clusterHandleManualFailover() {
        if (server.cluster.mfEnd == 0) return;
        if (!server.cluster.mfCanStart) return;

        if (server.cluster.mfMasterOffset == 0) return;

        if (server.cluster.mfMasterOffset == replicationManager.replicationGetSlaveOffset()) {
            server.cluster.mfCanStart = true;
            logger.warn("All master replication stream processed, manual failover can start.");
        }
    }

    public void clusterBeforeSleep() {
        if ((server.cluster.todoBeforeSleep & CLUSTER_TODO_HANDLE_FAILOVER) != 0)
            clusterHandleSlaveFailover();

        if ((server.cluster.todoBeforeSleep & CLUSTER_TODO_UPDATE_STATE) != 0)
            clusterUpdateState();

        if ((server.cluster.todoBeforeSleep & CLUSTER_TODO_SAVE_CONFIG) != 0) {
            configManager.clusterSaveConfigOrDie();
        }

        server.cluster.todoBeforeSleep = 0;
    }

    void clusterDoBeforeSleep(int flags) {
        server.cluster.todoBeforeSleep |= flags;
    }

    public static long amongMinorityTime = 0;
    public static long firstCallTime = 0;

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

    public boolean verifyClusterConfigWithData() {
        boolean updateConfig = false;

        if (nodeIsSlave(myself)) return true;

        for (int i = 0; i < CLUSTER_SLOTS; i++) {
            if (slotManger.countKeysInSlot(i) == 0) continue;
            if (server.cluster.slots[i].equals(myself) || server.cluster.importingSlotsFrom[i] != null) continue;

            updateConfig = true;
            if (server.cluster.slots[i] == null) {
                logger.warn("I have keys for unassigned slot " + i + ". Taking responsibility for it.");
                slotManger.clusterAddSlot(myself, i);
            } else {
                logger.warn("I have keys for slot " + i + ", but the slot is assigned to another node. Setting it to importing state.");
                server.cluster.importingSlotsFrom[i] = server.cluster.slots[i];
            }
        }
        if (updateConfig) configManager.clusterSaveConfigOrDie();
        return true;
    }

    public void clusterSetMaster(ClusterNode n) {
        if (nodeIsMaster(myself)) {
            myself.flags &= ~(CLUSTER_NODE_MASTER | CLUSTER_NODE_MIGRATE_TO);
            myself.flags |= CLUSTER_NODE_SLAVE;
            slotManger.clusterCloseAllSlots();
        } else {
            if (myself.slaveof != null)
                nodeManager.clusterNodeRemoveSlave(myself.slaveof, myself);
        }
        myself.slaveof = n;
        nodeManager.clusterNodeAddSlave(n, myself);
        replicationManager.replicationSetMaster(n.ip, n.port);
        resetManualFailover();
    }

    public static long iteration = 0;
    public static String prevIp = null;

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
                //TODO fd.setEncoder();
                //TODO fd.setDecoder();
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

            if (server.cluster.mfEnd != 0 && nodeIsMaster(myself) && server.cluster.mfSlave.equals(node) && node.link != null) {
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

        if (nodeIsSlave(myself) && server.masterhost == null &&
                myself.slaveof != null && nodeHasAddr(myself.slaveof)) {
            replicationManager.replicationSetMaster(myself.slaveof.ip, myself.slaveof.port);
        }

        manualFailoverCheckTimeout();

        if (nodeIsSlave(myself)) {
            clusterHandleManualFailover();
            clusterHandleSlaveFailover();
            if (orphanedMasters != 0 && maxSlaves >= 2 && thisSlaves == maxSlaves)
                clusterHandleSlaveMigration(maxSlaves);
        }

        if (updateState || server.cluster.state == CLUSTER_FAIL)
            clusterUpdateState();
    }

}

package com.moilioncircle.replicator.cluster.gossip;

import com.moilioncircle.replicator.cluster.ClusterLink;
import com.moilioncircle.replicator.cluster.ClusterNode;
import com.moilioncircle.replicator.cluster.ClusterNodeFailReport;
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

import java.io.*;
import java.util.*;
import java.util.concurrent.ExecutionException;

import static com.moilioncircle.replicator.cluster.ClusterConstants.*;
import static com.moilioncircle.replicator.cluster.config.ConfigFileParser.parseLine;
import static java.lang.Integer.parseInt;

/**
 * Created by Baoyi Chen on 2017/7/6.
 */
public class Gossip1 {
    private static final Log logger = LogFactory.getLog(Gossip1.class);

    private ClusterNode myself;

    private Server server = new Server();

    public boolean clusterLoadConfig(String fileName) {
        try (BufferedReader r = new BufferedReader(new FileReader(new File(fileName)))) {
            String line;
            while ((line = r.readLine()) != null) {
                if (line.length() == 0 || line.equals("\n")) continue;
                List<String> list = parseLine(line);
                if (list.isEmpty()) continue;
                if (list.get(0).equals("vars")) {
                    for (int i = 1; i < list.size(); i += 2) {
                        if (list.get(i).equals("currentEpoch")) {
                            server.cluster.currentEpoch = parseInt(list.get(i + 1));
                        } else if (list.get(i).equals("lastVoteEpoch")) {
                            server.cluster.lastVoteEpoch = parseInt(list.get(i + 1));
                        } else {
                            logger.warn("Skipping unknown cluster config variable '" + list.get(i) + "'");
                        }
                    }
                    continue;
                } else if (list.size() < 8) {
                    throw new UnsupportedOperationException("Unrecoverable error: corrupted cluster config file.");
                } else {
                    ClusterNode n = clusterLookupNode(list.get(0));
                    if (n == null) {
                        n = createClusterNode(list.get(0), 0);
                        clusterAddNode(n);
                    }
                    String hostAndPort = list.get(1);
                    if (!hostAndPort.contains(":")) {
                        throw new UnsupportedOperationException("Unrecoverable error: corrupted cluster config file.");
                    }
                    int colonIdx = hostAndPort.indexOf(":");
                    int atIdx = hostAndPort.indexOf("@");
                    n.ip = hostAndPort.substring(0, colonIdx);
                    n.port = parseInt(hostAndPort.substring(colonIdx, atIdx == -1 ? hostAndPort.length() : atIdx));
                    n.cport = atIdx == -1 ? n.port + CLUSTER_PORT_INCR : parseInt(hostAndPort.substring(atIdx));
                    String[] roles = list.get(2).split(",");
                    for (String role : roles) {
                        if (role.equals("myself")) {
                            myself = server.cluster.myself = n;
                            n.flags |= CLUSTER_NODE_MYSELF;
                        } else if (role.equals("master")) {
                            n.flags |= CLUSTER_NODE_MASTER;
                        } else if (role.equals("slave")) {
                            n.flags |= CLUSTER_NODE_SLAVE;
                        } else if (role.equals("fail?")) {
                            n.flags |= CLUSTER_NODE_PFAIL;
                        } else if (role.equals("fail")) {
                            n.flags |= CLUSTER_NODE_FAIL;
                            n.failTime = System.currentTimeMillis();
                        } else if (role.equals("handshake")) {
                            n.flags |= CLUSTER_NODE_HANDSHAKE;
                        } else if (role.equals("noaddr")) {
                            n.flags |= CLUSTER_NODE_NOADDR;
                        } else if (role.equals("noflags")) {
                            // NOP
                        } else {
                            throw new UnsupportedOperationException("Unknown flag in redis cluster config file");
                        }
                    }

                    ClusterNode master;
                    if (!list.get(3).equals("-")) {
                        master = clusterLookupNode(list.get(3));
                        if (master == null) {
                            master = createClusterNode(list.get(3), 0);
                            clusterAddNode(master);
                        }
                        n.slaveof = master;
                        clusterNodeAddSlave(master, n);
                    }

                    if (parseInt(list.get(4)) > 0) n.pingSent = System.currentTimeMillis();
                    if (parseInt(list.get(5)) > 0) n.pongReceived = System.currentTimeMillis();
                    n.configEpoch = parseInt(list.get(6));

                    for (int i = 8; i < list.size(); i++) {
                        int start = 0, stop = 0;
                        String argi = list.get(i);
                        char[] ary = argi.toCharArray();
                        if (ary[0] == '[') {
                            // [slot_number-<-importing_from_node_id]
                            if (argi.contains("-")) {
                                int idx = argi.indexOf("-");
                                char direction = ary[idx + 1];
                                int slot = parseInt(argi.substring(1, idx));
                                String p = argi.substring(idx + 3);
                                ClusterNode cn = clusterLookupNode(p);
                                if (cn == null) {
                                    cn = createClusterNode(p, 0);
                                    clusterAddNode(cn);
                                }
                                if (direction == '>') {
                                    server.cluster.migratingSlotsTo[slot] = cn;
                                } else {
                                    server.cluster.importingSlotsFrom[slot] = cn;
                                }
                                continue;
                            }
                        } else if (argi.contains("-")) {
                            int idx = argi.indexOf("-");
                            start = parseInt(argi.substring(0, idx));
                            stop = parseInt(argi.substring(idx));
                        } else {
                            start = stop = parseInt(argi);
                        }
                        while (start <= stop) clusterAddSlot(n, start++);
                    }
                }
            }
            if (server.cluster.myself == null) {
                throw new UnsupportedOperationException("Unrecoverable error: corrupted cluster config file.");
            }
            logger.info("Node configuration loaded, I'm " + myself.name);

            if (clusterGetMaxEpoch() > server.cluster.currentEpoch) {
                server.cluster.currentEpoch = clusterGetMaxEpoch();
            }
            return true;
        } catch (IOException e) {
            return false;
        }
    }

    public boolean clusterSaveConfig() {
        server.cluster.todoBeforeSleep &= ~CLUSTER_TODO_SAVE_CONFIG;

        BufferedWriter r = null;
        try {
            File file = new File(server.clusterConfigfile);
            if (!file.exists()) file.createNewFile();
            r = new BufferedWriter(new FileWriter(file));
            StringBuilder ci = new StringBuilder();
            ci.append(clusterGenNodesDescription(CLUSTER_NODE_HANDSHAKE));
            ci.append("vars currentEpoch ").append(server.cluster.currentEpoch);
            ci.append(" lastVoteEpoch ").append(server.cluster.lastVoteEpoch);
            r.write(ci.toString());
            r.flush();
            return true;
        } catch (IOException e) {
            return false;
        } finally {
            if (r != null) try {
                r.close();
            } catch (IOException e) {
            }
        }
    }

    public void clusterSaveConfigOrDie() {
        if (clusterSaveConfig()) return;
        throw new UnsupportedOperationException("Fatal: can't update cluster config file.");
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
        clusterCloseAllSlots();

        boolean saveconf = false;
        if (!clusterLoadConfig(server.clusterConfigfile)) {
            myself = server.cluster.myself = createClusterNode(null, CLUSTER_NODE_MYSELF | CLUSTER_NODE_MASTER);
            logger.info("No cluster configuration found, I'm " + myself.name);
            clusterAddNode(myself);
            saveconf = true;
        }
        if (saveconf) clusterSaveConfigOrDie();

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
            replicationUnsetMaster();
        }

        clusterCloseAllSlots();
        resetManualFailover();

        for (int i = 0; i < CLUSTER_SLOTS; i++) clusterDelSlot(i);

        for (Map.Entry<String, ClusterNode> entry : server.cluster.nodes.entrySet()) {
            ClusterNode node = entry.getValue();
            if (node.equals(myself)) continue;
            clusterDelNode(node);
        }

        if (force) {
            server.cluster.currentEpoch = 0;
            server.cluster.lastVoteEpoch = 0;
            myself.configEpoch = 0;
            logger.warn("configEpoch set to 0 via CLUSTER RESET HARD");
            String old = myself.name;
            server.cluster.nodes.remove(old);
            myself.name = getRandomHexChars(CLUSTER_NAMELEN);
            clusterAddNode(myself);
            logger.info("Node hard reset, now I'm " + myself.name);
        }

        clusterDoBeforeSleep(CLUSTER_TODO_SAVE_CONFIG | CLUSTER_TODO_UPDATE_STATE);
    }

    private void replicationUnsetMaster() {
        //TODO
    }

    public static final char[] chars = new char[]{'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'};

    private String getRandomHexChars(int clusterNamelen) {
        StringBuilder r = new StringBuilder();
        for (int i = 0; i < clusterNamelen; i++) {
            r.append(chars[new Random().nextInt(chars.length)]);
        }
        return r.toString();
    }

    public ClusterLink createClusterLink(ClusterNode node) {
        ClusterLink link = new ClusterLink();
        link.ctime = System.currentTimeMillis();
        link.node = node;
        return link;
    }

    public void freeClusterLink(ClusterLink link) {
        if (link.node != null) {
            link.node.link = null;
        }
        link.fd.disconnect(null);
    }

    public void clusterAcceptHandler(Transport<Message> transport, Message message) {
        ClusterLink link = createClusterLink(null);
        link.fd = new SessionImpl<>(transport);
        clusterProcessPacket(link, message);
    }

    public int keyHashSlot(String key) {
        int st = key.indexOf('{');
        if (st < 0) return crc16(key) & 0x3FFF;
        int ed = key.indexOf('}');
        if (ed < 0 || ed == st + 1) return crc16(key) & 0x3FFF; //{}
        if (st > ed) return crc16(key) & 0x3FFF; //}{
        return crc16(key.substring(st + 1, ed)) & 0x3FFF;
    }

    private int crc16(String key) {
        //TODO
        return 0;
    }

    public ClusterNode createClusterNode(String nodename, int flags) {
        ClusterNode node = new ClusterNode();
        if (nodename != null) {
            node.name = nodename;
        } else {
            node.name = getRandomHexChars(CLUSTER_NAMELEN);
        }

        node.ctime = System.currentTimeMillis();
        node.configEpoch = 0;
        node.flags = flags;
        node.numslots = 0;
        node.numslaves = 0;
        node.slaves = null;
        node.slaveof = null;
        node.pingSent = node.pongReceived = 0;
        node.failTime = 0;
        node.link = null;
        node.ip = null;
        node.port = 0;
        node.cport = 0;
        node.failReports = new ArrayList<>();
        node.votedTime = 0;
        node.orphanedTime = 0;
        node.replOffsetTime = 0;
        node.replOffset = 0;
        return node;
    }

    public boolean clusterNodeAddFailureReport(ClusterNode failing, ClusterNode sender) {
        for (ClusterNodeFailReport n : failing.failReports) {
            if (!n.node.equals(sender)) continue;
            n.time = System.currentTimeMillis();
            return false;
        }

        ClusterNodeFailReport fr = new ClusterNodeFailReport();
        fr.node = sender;
        fr.time = System.currentTimeMillis();
        failing.failReports.add(fr);
        return true;
    }

    public void clusterNodeCleanupFailureReports(ClusterNode node) {
        List<ClusterNodeFailReport> l = node.failReports;
        long max = server.clusterNodeTimeout * CLUSTER_FAIL_REPORT_VALIDITY_MULT;
        long now = System.currentTimeMillis();
        Iterator<ClusterNodeFailReport> it = l.iterator();
        while (it.hasNext()) {
            ClusterNodeFailReport n = it.next();
            if (now - n.time > max) it.remove();
        }
    }

    public boolean clusterNodeDelFailureReport(ClusterNode node, ClusterNode sender) {
        ClusterNodeFailReport fr = null;
        for (ClusterNodeFailReport n : node.failReports) {
            if (!n.node.equals(sender)) continue;
            fr = n;
            break;
        }
        if (fr == null) return false;
        node.failReports.remove(fr);
        clusterNodeCleanupFailureReports(node);
        return true;
    }

    public int clusterNodeFailureReportsCount(ClusterNode node) {
        clusterNodeCleanupFailureReports(node);
        return node.failReports.size();
    }

    public boolean clusterNodeRemoveSlave(ClusterNode master, ClusterNode slave) {
        Iterator<ClusterNode> it = master.slaves.iterator();
        while (it.hasNext()) {
            if (!it.next().equals(slave)) continue;
            it.remove();
            if (--master.numslaves == 0) {
                master.flags &= ~CLUSTER_NODE_MIGRATE_TO;
            }
            return true;
        }
        return false;
    }

    public boolean clusterNodeAddSlave(ClusterNode master, ClusterNode slave) {
        for (ClusterNode n : master.slaves) {
            if (n.equals(slave)) return false;
        }
        master.slaves.add(slave);
        master.numslaves++;
        master.flags |= CLUSTER_NODE_MIGRATE_TO;
        return true;
    }

    public int clusterCountNonFailingSlaves(ClusterNode n) {
        int count = 0;
        for (ClusterNode s : n.slaves) {
            if (!nodeFailed(s)) count++;
        }
        return count;
    }

    public void freeClusterNode(ClusterNode n) {
        for (ClusterNode r : n.slaves) {
            r.slaveof = null;
        }

        if (nodeIsSlave(n) && n.slaveof != null) clusterNodeRemoveSlave(n.slaveof, n);

        server.cluster.nodes.remove(n.name);
        if (n.link != null) freeClusterLink(n.link);
        n.failReports.clear();
        n.slaves.clear();
    }

    public boolean clusterAddNode(ClusterNode node) {
        ClusterNode old = server.cluster.nodes.put(node.name, node);
        return old == null;
    }

    public void clusterDelNode(ClusterNode delnode) {
        for (int i = 0; i < CLUSTER_SLOTS; i++) {
            if (server.cluster.importingSlotsFrom[i].equals(delnode))
                server.cluster.importingSlotsFrom[i] = null;
            if (server.cluster.migratingSlotsTo[i].equals(delnode)) {
                server.cluster.migratingSlotsTo[i] = null;
            }
            if (server.cluster.slots[i].equals(delnode)) {
                clusterDelSlot(i);
            }
        }

        for (ClusterNode node : server.cluster.nodes.values()) {
            if (node.equals(delnode)) continue;
            clusterNodeDelFailureReport(node, delnode);
        }
        freeClusterNode(delnode);
    }

    public ClusterNode clusterLookupNode(String name) {
        return server.cluster.nodes.get(name);
    }

    public void clusterRenameNode(ClusterNode node, String newname) {
        logger.debug("Renaming node " + node.name + " into " + newname);
        server.cluster.nodes.remove(node.name);
        node.name = newname;
        clusterAddNode(node);
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
        clusterSaveConfigOrDie();

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

        int failures = clusterNodeFailureReportsCount(node);

        if (nodeIsMaster(myself)) failures++;
        if (failures < neededQuorum) return;

        logger.info("Marking node " + node.name + " as failing (quorum reached).");

        node.flags &= ~CLUSTER_NODE_PFAIL;
        node.flags |= CLUSTER_NODE_FAIL;
        node.failTime = System.currentTimeMillis();

        if (nodeIsMaster(myself)) clusterSendFail(node.name);
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

        ClusterNode n = createClusterNode(null, CLUSTER_NODE_HANDSHAKE | CLUSTER_NODE_MEET);
        n.ip = ip;
        n.port = port;
        n.cport = cport;
        clusterAddNode(n);
        return true;
    }

    public void clusterProcessGossipSection(ClusterMsg hdr, ClusterLink link) {
        ClusterMsgDataGossip[] gs = hdr.data.gossip;
        ClusterNode sender = link.node != null ? link.node : clusterLookupNode(hdr.sender);
        for (ClusterMsgDataGossip g : gs) {
            int flags = g.flags;
            String ci = representClusterNodeFlags(flags);
            logger.debug("GOSSIP " + g.nodename + " " + g.ip + ":" + g.port + "@" + g.cport + " " + ci);

            ClusterNode node = clusterLookupNode(g.nodename);

            if (node == null) {
                if (sender != null && (flags & CLUSTER_NODE_NOADDR) == 0 && !clusterBlacklistExists(g.nodename)) {
                    clusterStartHandshake(g.ip, g.port, g.cport);
                }
                continue;
            }

            if (sender != null && nodeIsMaster(sender) && !node.equals(myself)) {
                if ((flags & (CLUSTER_NODE_FAIL | CLUSTER_NODE_PFAIL)) != 0) {
                    if (clusterNodeAddFailureReport(node, sender)) {
                        logger.debug("Node " + sender.name + " reported node " + node.name + " as not reachable.");
                    }
                    markNodeAsFailingIfNeeded(node);
                } else {
                    if (clusterNodeDelFailureReport(node, sender)) {
                        logger.debug("Node " + sender.name + " reported node " + node.name + " is back online.");
                    }
                }
            }

            if ((flags & (CLUSTER_NODE_FAIL | CLUSTER_NODE_PFAIL)) == 0 && node.pingSent == 0 && clusterNodeFailureReportsCount(node) == 0) {
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
                if (node.link != null) freeClusterLink(node.link);
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
        if (node.link != null) freeClusterLink(node.link);
        logger.warn("Address updated for node " + node.name + ", now " + node.ip + ":" + node.port);

        if (nodeIsSlave(myself) && myself.slaveof.equals(node)) {
            replicationSetMaster(node.ip, node.port);
        }
        return true;
    }

    private void replicationSetMaster(String ip, int port) {
        //TODO
    }

    public void clusterSetNodeAsMaster(ClusterNode n) {
        if (nodeIsMaster(n)) return;

        if (n.slaveof != null) {
            clusterNodeRemoveSlave(n.slaveof, n);
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
            if (bitmapTestBit(slots, i)) {
                if (server.cluster.slots[i].equals(sender)) continue;

                if (server.cluster.importingSlotsFrom[i] != null) continue;

                if (server.cluster.slots[i] == null || server.cluster.slots[i].configEpoch < senderConfigEpoch) {
                    if (server.cluster.slots[i].equals(myself) && countKeysInSlot(i) && !sender.equals(myself)) {
                        dirtySlots[dirtySlotsCount] = i;
                        dirtySlotsCount++;
                    }

                    if (server.cluster.slots[i].equals(curmaster))
                        newmaster = sender;
                    clusterDelSlot(i);
                    clusterAddSlot(sender, i);
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
                delKeysInSlot(dirtySlots[i]);
        }
    }

    private void delKeysInSlot(int dirtySlot) {
        //TODO
    }

    private boolean countKeysInSlot(int i) {
        //TODO
        return false;
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

        ClusterNode sender = clusterLookupNode(hdr.sender);
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
                ClusterNode node = createClusterNode(null, CLUSTER_NODE_HANDSHAKE);
                node.ip = nodeIp2String(link, hdr.myip);
                node.port = hdr.port;
                node.cport = hdr.cport;
                clusterAddNode(node);
                clusterDoBeforeSleep(CLUSTER_TODO_SAVE_CONFIG);
            }

            if (sender == null && type == CLUSTERMSG_TYPE_MEET) {
                clusterProcessGossipSection(hdr, link);
            }

            clusterSendPing(link, CLUSTERMSG_TYPE_PONG);
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
                        clusterDelNode(link.node);
                        return false;
                    }

                    clusterRenameNode(link.node, hdr.sender);
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
                    freeClusterLink(link);
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
                    ClusterNode master = clusterLookupNode(hdr.slaveof);

                    if (nodeIsMaster(sender)) {
                        clusterDelNodeSlots(sender);
                        sender.flags &= ~(CLUSTER_NODE_MASTER | CLUSTER_NODE_MIGRATE_TO);
                        sender.flags |= CLUSTER_NODE_SLAVE;

                        clusterDoBeforeSleep(CLUSTER_TODO_SAVE_CONFIG | CLUSTER_TODO_UPDATE_STATE);
                    }

                    if (master != null && !sender.slaveof.equals(master)) {
                        if (sender.slaveof != null) clusterNodeRemoveSlave(sender.slaveof, sender);
                        clusterNodeAddSlave(master, sender);
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
                    if (bitmapTestBit(hdr.myslots, i)) {
                        if (server.cluster.slots[i].equals(sender) || server.cluster.slots[i] == null) continue;
                        if (server.cluster.slots[i].configEpoch > senderConfigEpoch) {
                            logger.debug("Node " + sender.name + " has old slots configuration, sending an UPDATE message about " + server.cluster.slots[i].name);
                            clusterSendUpdate(sender.link, server.cluster.slots[i]);
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
                ClusterNode failing = clusterLookupNode(hdr.data.about.nodename);
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
            clusterSendFailoverAuthIfNeeded(sender, hdr);
        } else if (type == CLUSTERMSG_TYPE_FAILOVER_AUTH_ACK) {
            if (sender == null) return true;
            if (nodeIsMaster(sender) && sender.numslots > 0 && senderCurrentEpoch >= server.cluster.failoverAuthEpoch) {
                server.cluster.failoverAuthEpoch++;
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
            ClusterNode n = clusterLookupNode(hdr.data.nodecfg.nodename);
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

    private void pauseClients(long l) {
        //TODO
    }

    public void handleLinkIOError(ClusterLink link) {
        freeClusterLink(link);
    }

    public void clusterSendMessage(ClusterLink link, ClusterMsg hdr) {
        link.fd.send(hdr);
        int type = hdr.type;
        if (type < CLUSTERMSG_TYPE_COUNT)
            server.cluster.statsBusMessagesSent[type]++;
    }

    public void clusterBroadcastMessage(ClusterMsg hdr) {
        for (ClusterNode node : server.cluster.nodes.values()) {
            if (node.link == null) continue;
            if ((node.flags & (CLUSTER_NODE_MYSELF | CLUSTER_NODE_HANDSHAKE)) != 0) continue;
            clusterSendMessage(node.link, hdr);
        }
    }

    public ClusterMsg clusterBuildMessageHdr(int type) {
        ClusterMsg hdr = new ClusterMsg();
        ClusterNode master = (nodeIsSlave(myself) && myself.slaveof != null) ? myself.slaveof : myself;
        hdr.ver = CLUSTER_PROTO_VER;
        hdr.sig = "RCmb";
        hdr.type = type;
        hdr.sender = myself.name;
        if (server.clusterAnnounceIp != null) {
            hdr.myip = server.clusterAnnounceIp;
        }

        int announcedPort = server.clusterAnnouncePort != 0 ? server.clusterAnnouncePort : server.port;
        int announcedCport = server.clusterAnnounceBusPort != 0 ? server.clusterAnnounceBusPort : (server.port + CLUSTER_PORT_INCR);
        hdr.myslots = master.slots;
        if (master.slaveof != null) {
            hdr.slaveof = myself.slaveof.name;
        }

        hdr.port = announcedPort;
        hdr.cport = announcedCport;
        hdr.flags = myself.flags;
        hdr.state = server.cluster.state;

        hdr.currentEpoch = server.cluster.currentEpoch;
        hdr.configEpoch = master.configEpoch;

        long offset = 0;
        if (nodeIsSlave(myself))
            offset = replicationGetSlaveOffset();
        else
            offset = server.masterReplOffset;
        hdr.offset = offset;

        if (nodeIsMaster(myself) && server.cluster.mfEnd != 0)
            hdr.mflags[0] |= CLUSTERMSG_FLAG0_PAUSED;

        // TODO hdr.totlen = xx;
        return hdr;
    }

    private long replicationGetSlaveOffset() {
        //TOOD
        return 0;
    }

    public boolean clusterNodeIsInGossipSection(ClusterMsg hdr, int count, ClusterNode n) {
        for (int i = 0; i < count; i++) {
            if (hdr.data.gossip[i].nodename.equals(n.name)) return true;
        }
        return false;
    }

    public void clusterSetGossipEntry(ClusterMsg hdr, int i, ClusterNode n) {
        ClusterMsgDataGossip gossip = hdr.data.gossip[i];
        gossip.nodename = n.name;
        gossip.pingSent = n.pingSent / 1000;
        gossip.pongReceived = n.pongReceived / 1000;
        gossip.ip = n.ip;
        gossip.port = n.port;
        gossip.cport = n.cport;
        gossip.flags = n.flags;
        gossip.notused1 = 0;
    }

    public void clusterSendPing(ClusterLink link, int type) {
        int gossipcount = 0;

        int freshnodes = server.cluster.nodes.size() - 2; //去掉当前节点和发送的目标节点

        int wanted = server.cluster.nodes.size() / 10;
        if (wanted < 3) wanted = 3;
        if (wanted > freshnodes) wanted = freshnodes;

        int pfailWanted = (int) server.cluster.statsPfailNodes;

        if (link.node != null && type == CLUSTERMSG_TYPE_PING)
            link.node.pingSent = System.currentTimeMillis();
        ClusterMsg hdr = clusterBuildMessageHdr(type);

        int maxiterations = wanted * 3;
        //不对所有节点发送消息，选取<=wanted个节点
        while (freshnodes > 0 && gossipcount < wanted && maxiterations-- > 0) {
            List<ClusterNode> list = new ArrayList<>(server.cluster.nodes.values());
            int idx = new Random().nextInt(list.size());
            ClusterNode t = list.get(idx);

            if (t.equals(myself)) continue;

            if ((t.flags & CLUSTER_NODE_PFAIL) != 0) continue;

            if ((t.flags & (CLUSTER_NODE_HANDSHAKE | CLUSTER_NODE_NOADDR)) != 0 || (t.link == null && t.numslots == 0)) {
//                freshnodes--;
                continue;
            }

            if (clusterNodeIsInGossipSection(hdr, gossipcount, t)) continue;

            clusterSetGossipEntry(hdr, gossipcount, t);
            freshnodes--;
            gossipcount++;
        }

        //把pfail节点加到最后
        if (pfailWanted != 0) {
            List<ClusterNode> list = new ArrayList<>(server.cluster.nodes.values());
            for (int i = 0; i < list.size() && pfailWanted > 0; i++) {
                ClusterNode node = list.get(i);
                if ((node.flags & CLUSTER_NODE_HANDSHAKE) != 0) continue;
                if ((node.flags & CLUSTER_NODE_NOADDR) != 0) continue;
                if ((node.flags & CLUSTER_NODE_PFAIL) == 0) continue;
                clusterSetGossipEntry(hdr, gossipcount, node);
                freshnodes--;
                gossipcount++;
                pfailWanted--;
            }
        }

        hdr.count = gossipcount;
        //TODO hdr.totlen = xx
        clusterSendMessage(link, hdr);
    }

    public void clusterBroadcastPong(int target) {
        for (ClusterNode node : server.cluster.nodes.values()) {
            if (node.link == null) continue;
            if (node.equals(myself) || nodeInHandshake(node)) continue;
            if (target == CLUSTER_BROADCAST_LOCAL_SLAVES) {
                boolean localSlave = nodeIsSlave(node) && node.slaveof != null && (node.slaveof.equals(myself) || node.slaveof.equals(myself.slaveof));
                if (!localSlave) continue;
            }
            clusterSendPing(node.link, CLUSTERMSG_TYPE_PONG);
        }
    }

    public void clusterSendFail(String nodename) {
        ClusterMsg hdr = clusterBuildMessageHdr(CLUSTERMSG_TYPE_FAIL);
        hdr.data.about.nodename = nodename;
        clusterBroadcastMessage(hdr);
    }

    public void clusterSendUpdate(ClusterLink link, ClusterNode node) {
        if (link == null) return;
        ClusterMsg hdr = clusterBuildMessageHdr(CLUSTERMSG_TYPE_UPDATE);
        hdr.data.nodecfg.nodename = node.name;
        hdr.data.nodecfg.configEpoch = node.configEpoch;
        hdr.data.nodecfg.slots = node.slots;
        clusterSendMessage(link, hdr);
    }

    public void clusterRequestFailoverAuth() {
        ClusterMsg hdr = clusterBuildMessageHdr(CLUSTERMSG_TYPE_FAILOVER_AUTH_REQUEST);
        if (server.cluster.mfEnd > 0) hdr.mflags[0] |= CLUSTERMSG_FLAG0_FORCEACK;
        clusterBroadcastMessage(hdr);
    }

    public void clusterSendFailoverAuth(ClusterNode node) {
        if (node.link == null) return;
        ClusterMsg hdr = clusterBuildMessageHdr(CLUSTERMSG_TYPE_FAILOVER_AUTH_ACK);
        clusterSendMessage(node.link, hdr);
    }

    public void clusterSendMFStart(ClusterNode node) {
        if (node.link == null) return;
        ClusterMsg hdr = clusterBuildMessageHdr(CLUSTERMSG_TYPE_MFSTART);
        clusterSendMessage(node.link, hdr);
    }

    public void clusterSendFailoverAuthIfNeeded(ClusterNode node, ClusterMsg request) {
        ClusterNode master = node.slaveof;
        long requestCurrentEpoch = request.currentEpoch;
        long requestConfigEpoch = request.configEpoch;
        byte[] claimedSlots = request.myslots;
        boolean forceAck = (request.mflags[0] & CLUSTERMSG_FLAG0_FORCEACK) != 0;

        if (nodeIsSlave(myself) || myself.numslots == 0) return;

        if (requestCurrentEpoch < server.cluster.currentEpoch) {
            logger.warn("Failover auth denied to " + node.name + ": reqEpoch (" + requestCurrentEpoch + ") < curEpoch(" + server.cluster.currentEpoch + ")");
            return;
        }

        if (server.cluster.lastVoteEpoch == server.cluster.currentEpoch) {
            logger.warn("Failover auth denied to " + node.name + ": already voted for epoch " + server.cluster.currentEpoch);
            return;
        }

        if (nodeIsMaster(node) || master == null || (!nodeFailed(master) && !forceAck)) {
            if (nodeIsMaster(node)) {
                logger.warn("Failover auth denied to " + node.name + ": it is a master node");
            } else if (master == null) {
                logger.warn("Failover auth denied to " + node.name + ": I don't know its master");
            } else if (!nodeFailed(master)) {
                logger.warn("Failover auth denied to " + node.name + ": its master is up");
            }
            return;
        }

        if (System.currentTimeMillis() - node.slaveof.votedTime < server.clusterNodeTimeout * 2) {
            logger.warn("Failover auth denied to " + node.name + ": can't vote about this master before " + (server.clusterNodeTimeout * 2 - System.currentTimeMillis() - node.slaveof.votedTime) + " milliseconds");
            return;
        }

        for (int i = 0; i < CLUSTER_SLOTS; i++) {
            if (!bitmapTestBit(claimedSlots, i)) continue;
            if (server.cluster.slots[i] == null || server.cluster.slots[i].configEpoch <= requestConfigEpoch) {
                continue;
            }
            logger.warn("Failover auth denied to " + node.name + ": slot " + i + " epoch (" + server.cluster.slots[i].configEpoch + ") > reqEpoch (" + requestConfigEpoch + ")");
            return;
        }

        clusterSendFailoverAuth(node);
        server.cluster.lastVoteEpoch = server.cluster.currentEpoch;
        node.slaveof.votedTime = System.currentTimeMillis();
        logger.warn("Failover auth granted to " + node.name + " for epoch " + server.cluster.currentEpoch);
    }

    public int clusterGetSlaveRank() {
        int rank = 0;
        ClusterNode master = myself.slaveof;
        if (master == null) return rank;
        long myoffset = replicationGetSlaveOffset();
        for (int i = 0; i < master.numslaves; i++)
            if (!master.slaves.get(i).equals(myself) && master.slaves.get(i).replOffset > myoffset)
                rank++;
        return rank;
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
        replicationUnsetMaster();

        for (int i = 0; i < CLUSTER_SLOTS; i++) {
            if (clusterNodeGetSlotBit(oldmaster, i)) {
                clusterDelSlot(i);
                clusterAddSlot(myself, i);
            }
        }

        clusterUpdateState();
        clusterSaveConfigOrDie();
        clusterBroadcastPong(CLUSTER_BROADCAST_ALL);

        resetManualFailover();
    }

    public void clusterHandleSlaveFailover() {

        long authAge = System.currentTimeMillis() - server.cluster.failoverAuthTime;
        int neededQuorum = (server.cluster.size / 2) + 1;
        boolean manualFailover = server.cluster.mfEnd != 0 && server.cluster.mfCanStart != 0;

        server.cluster.todoBeforeSleep &= ~CLUSTER_TODO_HANDLE_FAILOVER;

        long authTimeout = server.clusterNodeTimeout * 2;
        if (authTimeout < 2000) authTimeout = 2000;
        long authRetryTime = authTimeout * 2;

        if (nodeIsMaster(myself) || myself.slaveof == null || (!nodeFailed(myself.slaveof) && !manualFailover) || myself.slaveof.numslots == 0) {
            server.cluster.cantFailoverReason = CLUSTER_CANT_FAILOVER_NONE;
            return;
        }

        long data_age;
        if (server.replState == REPL_STATE_CONNECTED) {
            data_age = (System.currentTimeMillis() - server.lastinteraction) * 1000;
        } else {
            data_age = (System.currentTimeMillis() - server.replDownSince) * 1000;
        }

        if (data_age > server.clusterNodeTimeout)
            data_age -= server.clusterNodeTimeout;

        if (server.clusterSlaveValidityFactor != 0 && data_age > (server.replPingSlavePeriod * 1000) + (server.clusterNodeTimeout * server.clusterSlaveValidityFactor)) {
            if (!manualFailover) {
                clusterLogCantFailover(CLUSTER_CANT_FAILOVER_DATA_AGE);
                return;
            }
        }

        if (authAge > authRetryTime) {
            server.cluster.failoverAuthTime = System.currentTimeMillis() +
                    500 + new Random().nextInt(500);
            server.cluster.failoverAuthCount = 0;
            server.cluster.failoverAuthSent = 0;
            server.cluster.failoverAuthRank = clusterGetSlaveRank();
            server.cluster.failoverAuthTime += server.cluster.failoverAuthRank * 1000;
            if (server.cluster.mfEnd != 0) {
                server.cluster.failoverAuthTime = System.currentTimeMillis();
                server.cluster.failoverAuthRank = 0;
            }
            logger.warn("Start of election delayed for " + (server.cluster.failoverAuthTime - System.currentTimeMillis()) + " milliseconds (rank #" + server.cluster.failoverAuthRank + ", offset " + replicationGetSlaveOffset() + ").");
            clusterBroadcastPong(CLUSTER_BROADCAST_LOCAL_SLAVES);
            return;
        }

        if (server.cluster.failoverAuthSent == 0 && server.cluster.mfEnd == 0) {
            int newrank = clusterGetSlaveRank();
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

        if (server.cluster.failoverAuthSent == 0) {
            server.cluster.currentEpoch++;
            server.cluster.failoverAuthEpoch = server.cluster.currentEpoch;
            logger.warn("Starting a failover election for epoch " + server.cluster.currentEpoch);
            clusterRequestFailoverAuth();
            server.cluster.failoverAuthSent = 1;
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

            if (nodeIsMaster(node)) okslaves = clusterCountNonFailingSlaves(node);
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
        server.cluster.mfCanStart = 0;
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
        if (server.cluster.mfCanStart == 0) return;

        if (server.cluster.mfMasterOffset == 0) return;

        if (server.cluster.mfMasterOffset == replicationGetSlaveOffset()) {
            server.cluster.mfCanStart = 1;
            logger.warn("All master replication stream processed, manual failover can start.");
        }
    }

    public void clusterBeforeSleep() {
        if ((server.cluster.todoBeforeSleep & CLUSTER_TODO_HANDLE_FAILOVER) != 0)
            clusterHandleSlaveFailover();

        if ((server.cluster.todoBeforeSleep & CLUSTER_TODO_UPDATE_STATE) != 0)
            clusterUpdateState();

        if ((server.cluster.todoBeforeSleep & CLUSTER_TODO_SAVE_CONFIG) != 0) {
            clusterSaveConfigOrDie();
        }

        server.cluster.todoBeforeSleep = 0;
    }

    void clusterDoBeforeSleep(int flags) {
        server.cluster.todoBeforeSleep |= flags;
    }

    public boolean bitmapTestBit(byte[] bitmap, int pos) {
        int offset = pos / 8;
        int bit = pos & 7;
        return (bitmap[offset] & (1 << bit)) != 0;
    }

    public void bitmapSetBit(byte[] bitmap, int pos) {
        int offset = pos / 8;
        int bit = pos & 7;
        bitmap[offset] |= 1 << bit;
    }

    public void bitmapClearBit(byte[] bitmap, int pos) {
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

    public void clusterCloseAllSlots() {
        server.cluster.migratingSlotsTo = new ClusterNode[CLUSTER_SLOTS];
        server.cluster.importingSlotsFrom = new ClusterNode[CLUSTER_SLOTS];
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
            if (!countKeysInSlot(i)) continue;
            if (server.cluster.slots[i].equals(myself) || server.cluster.importingSlotsFrom[i] != null) continue;

            updateConfig = true;
            if (server.cluster.slots[i] == null) {
                logger.warn("I have keys for unassigned slot " + i + ". Taking responsibility for it.");
                clusterAddSlot(myself, i);
            } else {
                logger.warn("I have keys for slot " + i + ", but the slot is assigned to another node. Setting it to importing state.");
                server.cluster.importingSlotsFrom[i] = server.cluster.slots[i];
            }
        }
        if (updateConfig) clusterSaveConfigOrDie();
        return true;
    }

    public void clusterSetMaster(ClusterNode n) {
        if (nodeIsMaster(myself)) {
            myself.flags &= ~(CLUSTER_NODE_MASTER | CLUSTER_NODE_MIGRATE_TO);
            myself.flags |= CLUSTER_NODE_SLAVE;
            clusterCloseAllSlots();
        } else {
            if (myself.slaveof != null)
                clusterNodeRemoveSlave(myself.slaveof, myself);
        }
        myself.slaveof = n;
        clusterNodeAddSlave(n, myself);
        replicationSetMaster(n.ip, n.port);
        resetManualFailover();
    }

    private static class RedisNodeFlags {
        public int flag;
        public String name;

        public RedisNodeFlags(int flag, String name) {
            this.flag = flag;
            this.name = name;
        }
    }

    public static RedisNodeFlags[] redisNodeFlags = new RedisNodeFlags[]{
            new RedisNodeFlags(CLUSTER_NODE_MYSELF, "myself,"),
            new RedisNodeFlags(CLUSTER_NODE_MASTER, "master,"),
            new RedisNodeFlags(CLUSTER_NODE_SLAVE, "slave,"),
            new RedisNodeFlags(CLUSTER_NODE_PFAIL, "fail?,"),
            new RedisNodeFlags(CLUSTER_NODE_FAIL, "fail,"),
            new RedisNodeFlags(CLUSTER_NODE_HANDSHAKE, "handshake,"),
            new RedisNodeFlags(CLUSTER_NODE_NOADDR, "noaddr,"),
    };

    public String representClusterNodeFlags(int flags) {
        StringBuilder builder = new StringBuilder();
        if (flags == 0) {
            builder.append("noflags,");
        } else {
            for (RedisNodeFlags rnf : redisNodeFlags) {
                if ((flags & rnf.flag) != 0) builder.append(rnf.name);
            }
        }
        builder.deleteCharAt(builder.length() - 1);
        return builder.toString();
    }

    public String clusterGenNodeDescription(ClusterNode node) {
        StringBuilder ci = new StringBuilder();

        ci.append(node.name).append(" ").append(node.ip).append(":").append(node.port).append("@").append(node.cport);
        ci.append(representClusterNodeFlags(node.flags));
        if (node.slaveof != null)
            ci.append(" ").append(node.slaveof.name).append(" ");
        else
            ci.append(" - ");

        ci.append(node.pingSent).append(" ").append(node.pongReceived).append(" ").append(node.configEpoch).append(" ").append((node.link != null || (node.flags & CLUSTER_NODE_MYSELF) != 0) ? "connected" : "disconnected");

        int start = -1;
        for (int i = 0; i < CLUSTER_SLOTS; i++) {
            boolean bit;

            if ((bit = clusterNodeGetSlotBit(node, i))) {
                if (start == -1) start = i;
            }
            if (start != -1 && (!bit || i == CLUSTER_SLOTS - 1)) {
                if (bit && i == CLUSTER_SLOTS - 1) i++;
                if (start == i - 1) {
                    ci.append(" ").append(start);
                } else {
                    ci.append(" ").append(start).append("-").append(i - 1);
                }
                start = -1;
            }
        }

        if ((node.flags & CLUSTER_NODE_MYSELF) != 0) {
            for (int i = 0; i < CLUSTER_SLOTS; i++) {
                if (server.cluster.migratingSlotsTo[i] != null) {
                    ci.append(" [").append(i).append("->-").append(server.cluster.migratingSlotsTo[i].name).append("]");
                } else if (server.cluster.importingSlotsFrom[i] != null) {
                    ci.append(" [").append(i).append("-<-").append(server.cluster.importingSlotsFrom[i].name).append("]");
                }
            }
        }
        return ci.toString();
    }

    public String clusterGenNodesDescription(int filter) {
        String ni = "";
        for (ClusterNode node : server.cluster.nodes.values()) {
            if ((node.flags & filter) != 0) continue;
            ni = clusterGenNodeDescription(node);
            ni += "\n";
        }
        return ni;
    }

    public String clusterGetMessageTypeString(int type) {
        switch (type) {
            case CLUSTERMSG_TYPE_PING:
                return "ping";
            case CLUSTERMSG_TYPE_PONG:
                return "pong";
            case CLUSTERMSG_TYPE_MEET:
                return "meet";
            case CLUSTERMSG_TYPE_FAIL:
                return "fail";
            case CLUSTERMSG_TYPE_PUBLISH:
                return "publish";
            case CLUSTERMSG_TYPE_FAILOVER_AUTH_REQUEST:
                return "auth-req";
            case CLUSTERMSG_TYPE_FAILOVER_AUTH_ACK:
                return "auth-ack";
            case CLUSTERMSG_TYPE_UPDATE:
                return "update";
            case CLUSTERMSG_TYPE_MFSTART:
                return "mfstart";
        }
        return "unknown";
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
                clusterDelNode(node);
                continue;
            }

            if (node.link == null) {
                final ClusterLink link = createClusterLink(node);
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

                clusterSendPing(link, (node.flags & CLUSTER_NODE_MEET) != 0 ? CLUSTERMSG_TYPE_MEET : CLUSTERMSG_TYPE_PING);
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
                clusterSendPing(minPongNode.link, CLUSTERMSG_TYPE_PING);
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
                int okslaves = clusterCountNonFailingSlaves(node);

                if (okslaves == 0 && node.numslots > 0 && (node.flags & CLUSTER_NODE_MIGRATE_TO) != 0) {
                    orphanedMasters++;
                }
                if (okslaves > maxSlaves) maxSlaves = okslaves;
                if (nodeIsSlave(myself) && myself.slaveof.equals(node))
                    thisSlaves = okslaves;
            }

            if (node.link != null && now - node.link.ctime > server.clusterNodeTimeout &&
                    node.pingSent != 0 && node.pongReceived < node.pingSent && now - node.pingSent > server.clusterNodeTimeout / 2) {
                freeClusterLink(node.link);
            }

            if (node.link != null && node.pingSent == 0 && (now - node.pongReceived) > server.clusterNodeTimeout / 2) {
                clusterSendPing(node.link, CLUSTERMSG_TYPE_PING);
                continue;
            }

            if (server.cluster.mfEnd != 0 && nodeIsMaster(myself) && server.cluster.mfSlave.equals(node) && node.link != null) {
                clusterSendPing(node.link, CLUSTERMSG_TYPE_PING);
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
            replicationSetMaster(myself.slaveof.ip, myself.slaveof.port);
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



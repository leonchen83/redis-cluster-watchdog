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
public class Gossip {
    private static final Log logger = LogFactory.getLog(Gossip.class);

    private ClusterNode myself;

    private Server server;

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
                    logger.warn("Unrecoverable error: corrupted cluster config file.");
                    System.exit(1);
                } else {
                    ClusterNode n = clusterLookupNode(list.get(0));
                    if (n == null) {
                        n = createClusterNode(list.get(0), 0);
                        clusterAddNode(n);
                    }
                    String hostAndPort = list.get(1);
                    if (!hostAndPort.contains(":")) {
                        logger.warn("Unrecoverable error: corrupted cluster config file.");
                        System.exit(1);
                    }
                    int coronIdx = hostAndPort.indexOf(":");
                    int atIdx = hostAndPort.indexOf("@");
                    n.ip = hostAndPort.substring(0, coronIdx);
                    n.port = parseInt(hostAndPort.substring(coronIdx, atIdx == -1 ? hostAndPort.length() : atIdx));
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
                            logger.warn("Unknown flag in redis cluster config file");
                            System.exit(1);
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
                            int slot = -1;
                            char direction;
                            ClusterNode cn;
                            // [slot_number-<-importing_from_node_id]
                            if (argi.contains("-")) {
                                int idx = argi.indexOf("-");
                                direction = ary[idx + 1];
                                slot = parseInt(argi.substring(1, idx));
                                String p = argi.substring(idx + 3)
                                cn = clusterLookupNode(p);
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
                logger.warn("Unrecoverable error: corrupted cluster config file.");
                System.exit(1);
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
            StringBuilder ci = clusterGenNodesDescription(CLUSTER_NODE_HANDSHAKE);
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
        if (!clusterSaveConfig()) {
            logger.warn("Fatal: can't update cluster config file.");
            System.exit(1);
        }
    }

    public void clusterInit() throws ExecutionException, InterruptedException {
        boolean saveconf = false;

        server.cluster = new ClusterState();
        server.cluster.myself = null;
        server.cluster.currentEpoch = 0;
        server.cluster.state = CLUSTER_FAIL;
        server.cluster.state = 1;
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
            }

            @Override
            public void onMessage(Transport<Message> transport, Message message) {
                server.cfd.add(new SessionImpl<>(transport));
                clusterAcceptHandler(transport, message);
            }

            @Override
            public void onException(Transport<Message> transport, Throwable cause) {
                logger.error(cause.getMessage());
            }

            @Override
            public void onDisconnected(Transport<Message> transport, Throwable cause) {
                logger.info("< " + transport.toString());
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

    void clusterReset(boolean hard) {
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

        if (hard) {
            server.cluster.currentEpoch = 0;
            server.cluster.lastVoteEpoch = 0;
            myself.configEpoch = 0;
            logger.warn("configEpoch set to 0 via CLUSTER RESET HARD");
            String oldname = myself.name;
            server.cluster.nodes.remove(oldname);
            myself.name = getRandomHexChars(CLUSTER_NAMELEN);
            clusterAddNode(myself);
            logger.info("Node hard reset, now I'm " + myself.name);
        }

        clusterDoBeforeSleep(CLUSTER_TODO_SAVE_CONFIG | CLUSTER_TODO_UPDATE_STATE);
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
        clusterReadHandler(link, message);
    }

    public int keyHashSlot(String key) {
        int keylen = key.length();
        int st = key.indexOf('{');
        if (st < 0) return crc16(key) & 0x3FFF;
        int ed = key.indexOf('}');
        if (ed < 0 || ed == st + 1) return crc16(key) & 0x3FFF;
        if (st > ed) return crc16(key) & 0x3FFF;
        return crc16(key.substring(st + 1, ed)) & 0x3FFF;
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
        List<ClusterNodeFailReport> l = failing.failReports;
        for (ClusterNodeFailReport ln : l) {
            if (ln.node.equals(sender)) {
                ln.time = System.currentTimeMillis();
                return false;
            }
        }

        ClusterNodeFailReport fr = new ClusterNodeFailReport();
        fr.node = sender;
        fr.time = System.currentTimeMillis();
        l.add(fr);
        return true;
    }

    public void clusterNodeCleanupFailureReports(ClusterNode node) {
        List<ClusterNodeFailReport> l = node.failReports;
        long maxtime = server.clusterNodeTimeout * CLUSTER_FAIL_REPORT_VALIDITY_MULT;
        long now = System.currentTimeMillis();
        Iterator<ClusterNodeFailReport> it = l.iterator();
        while (it.hasNext()) {
            ClusterNodeFailReport ln = it.next();
            if (now - ln.time > maxtime) it.remove();
        }
    }

    public boolean clusterNodeDelFailureReport(ClusterNode node, ClusterNode sender) {
        List<ClusterNodeFailReport> l = node.failReports;
        ClusterNodeFailReport fr = null;
        for (ClusterNodeFailReport ln : l) {
            if (ln.node.equals(sender)) {
                fr = ln;
                break;
            }
        }
        if (fr == null) return false;
        l.remove(fr);
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
            if (it.next().equals(slave)) {
                it.remove();
                if (--master.numslaves == 0) {
                    master.flags &= ~CLUSTER_NODE_MIGRATE_TO;
                }
                return true;
            }
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
        int okslaves = 0;
        for (ClusterNode s : n.slaves) {
            if (!nodeFailed(s)) okslaves++;
        }
        return okslaves;
    }

    public void freeClusterNode(ClusterNode n) {
        for (ClusterNode r : n.slaves) {
            r.slaveof = null;
        }

        if (nodeIsSlave(n) && n.slaveof != null) clusterNodeRemoveSlave(n.slaveof, n);

        String nodename = n.name;
        server.cluster.nodes.remove(nodename);

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

    public boolean clusterBumpConfigEpochWithoutConsensus() {
        long maxEpoch = clusterGetMaxEpoch();

        if (myself.configEpoch == 0 || myself.configEpoch != maxEpoch) {
            server.cluster.currentEpoch++;
            myself.configEpoch = server.cluster.currentEpoch;
            clusterDoBeforeSleep(CLUSTER_TODO_SAVE_CONFIG);
            logger.warn("New configEpoch set to " + myself.configEpoch);
            return true;
        }
        return false;
    }

    public void clusterHandleConfigEpochCollision(ClusterNode sender) {
        if (sender.configEpoch != myself.configEpoch || !nodeIsMaster(sender) || !nodeIsMaster(myself)) return;

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
            if (expire < System.currentTimeMillis()) {
                it.remove();
            }
        }
    }

    public void clusterBlacklistAddNode(ClusterNode node) {
        clusterBlacklistCleanup();
        ClusterNode de = server.cluster.nodesBlackList.get(node.name);
        dictSetUnsignedIntegerVal(de, System.currentTimeMillis() + CLUSTER_BLACKLIST_TTL * 1000);
    }

    public boolean clusterBlacklistExists(String nodeid) {
        clusterBlacklistCleanup();
        return server.cluster.nodesBlackList.containsKey(nodeid);
    }

    public void markNodeAsFailingIfNeeded(ClusterNode node) {
        int neededQuorum = server.cluster.size / 2 + 1;

        if (!nodeTimedOut(node)) return;
        if (nodeFailed(node)) return;

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
            if (!nodeInHandshake(node)) continue;
            if (node.ip.equalsIgnoreCase(ip) && node.port == port && node.cport == cport) {
                return true;
            }
        }
        return false;
    }

    public boolean clusterStartHandshake(String ip, int port, int cport) {
        if (clusterHandshakeInProgress(ip, port, cport)) {
            return false;
        }

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
                if (sender != null && flags & CLUSTER_NODE_NOADDR != 0 && !clusterBlacklistExists(g.nodename)) {
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

            if ((flags & (CLUSTER_NODE_FAIL | CLUSTER_NODE_PFAIL)) != 0 && node.pingSent == 0 && clusterNodeFailureReportsCount(node) == 0) {
                long pongtime = g.pongReceived * 1000;
                if (pongtime <= (System.currentTimeMillis() + 500) && pongtime > node.pongReceived) {
                    node.pongReceived = pongtime;
                }
            }

            if ((node.flags & (CLUSTER_NODE_FAIL | CLUSTER_NODE_PFAIL)) == 0 && (flags & CLUSTER_NODE_NOADDR) != 0 && (flags & (CLUSTER_NODE_FAIL | CLUSTER_NODE_PFAIL)) != 0 &&
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

        if (node.link != null) freeClusterLink(node.link);
        logger.warn("Address updated for node " + node.name + ", now " + node.ip + ":" + node.port);

        if (nodeIsSlave(myself) && myself.slaveof.equals(node)) {
            replicationSetMaster(node.ip, node.port);
        }
        return true;
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
                ClusterNode node = createClusterNode(null, CLUSTER_NODE_HANDSHAKE) {
                    node.ip = nodeIp2String(link, hdr.myip);
                    node.port = hdr.port;
                    node.cport = hdr.cport;
                    clusterAddNode(node);
                    clusterDoBeforeSleep(CLUSTER_TODO_SAVE_CONFIG);
                }
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
                if (hdr.slaveof.equals(CLUSTER_NODE_NULL_NAME)) {
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
                ClusterNode failing = clusterLookupNode(hdr.data.fail.about.nodename);
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

    public void handleLinkIOError(ClusterLink link) {
        freeClusterLink(link);
    }

}




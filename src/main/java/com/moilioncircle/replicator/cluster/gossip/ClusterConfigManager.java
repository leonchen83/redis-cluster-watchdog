package com.moilioncircle.replicator.cluster.gossip;

import com.moilioncircle.replicator.cluster.ClusterNode;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.*;
import java.util.List;

import static com.moilioncircle.replicator.cluster.ClusterConstants.*;
import static com.moilioncircle.replicator.cluster.config.ConfigFileParser.parseLine;
import static java.lang.Integer.parseInt;

/**
 * Created by Baoyi Chen on 2017/7/12.
 */
public class ClusterConfigManager {
    private static final Log logger = LogFactory.getLog(ClusterConfigManager.class);
    private Server server;
    private ThinGossip gossip;
    private ClusterNode myself;

    public ClusterConfigManager(ThinGossip gossip) {
        this.gossip = gossip;
        this.server = gossip.server;
        this.myself = gossip.myself;
    }

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
                    ClusterNode n = gossip.nodeManager.clusterLookupNode(list.get(0));
                    if (n == null) {
                        n = gossip.nodeManager.createClusterNode(list.get(0), 0);
                        gossip.nodeManager.clusterAddNode(n);
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
                        master = gossip.nodeManager.clusterLookupNode(list.get(3));
                        if (master == null) {
                            master = gossip.nodeManager.createClusterNode(list.get(3), 0);
                            gossip.nodeManager.clusterAddNode(master);
                        }
                        n.slaveof = master;
                        gossip.nodeManager.clusterNodeAddSlave(master, n);
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
                                ClusterNode cn = gossip.nodeManager.clusterLookupNode(p);
                                if (cn == null) {
                                    cn = gossip.nodeManager.createClusterNode(p, 0);
                                    gossip.nodeManager.clusterAddNode(cn);
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
                        while (start <= stop) gossip.slotManger.clusterAddSlot(n, start++);
                    }
                }
            }
            if (server.cluster.myself == null) {
                throw new UnsupportedOperationException("Unrecoverable error: corrupted cluster config file.");
            }
            logger.info("Node configuration loaded, I'm " + myself.name);

            if (gossip.clusterGetMaxEpoch() > server.cluster.currentEpoch) {
                server.cluster.currentEpoch = gossip.clusterGetMaxEpoch();
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

            if ((bit = gossip.slotManger.clusterNodeGetSlotBit(node, i))) {
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
}

package com.moilioncircle.replicator.cluster.gossip;

import com.moilioncircle.replicator.cluster.ClusterNode;
import com.moilioncircle.replicator.cluster.Server;
import com.moilioncircle.replicator.cluster.config.ConfigInfo;
import com.moilioncircle.replicator.cluster.config.NodeInfo;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.*;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static com.moilioncircle.replicator.cluster.ClusterConstants.*;
import static com.moilioncircle.replicator.cluster.config.ConfigFileParser.parseLine;
import static com.moilioncircle.replicator.cluster.gossip.ClusterSlotManger.bitmapTestBit;
import static java.lang.Integer.parseInt;
import static java.lang.Long.parseLong;

/**
 * Created by Baoyi Chen on 2017/7/12.
 */
public class ClusterConfigManager {
    private static final Log logger = LogFactory.getLog(ClusterConfigManager.class);
    private Server server;
    private ThinGossip gossip;

    public ClusterConfigManager(ThinGossip gossip) {
        this.gossip = gossip;
        this.server = gossip.server;
    }

    public static Map<Integer, String> redisNodeFlags = new LinkedHashMap<>();

    static {
        redisNodeFlags.put(CLUSTER_NODE_MYSELF, "myself,");
        redisNodeFlags.put(CLUSTER_NODE_MASTER, "master,");
        redisNodeFlags.put(CLUSTER_NODE_SLAVE, "slave,");
        redisNodeFlags.put(CLUSTER_NODE_PFAIL, "fail?,");
        redisNodeFlags.put(CLUSTER_NODE_FAIL, "fail,");
        redisNodeFlags.put(CLUSTER_NODE_HANDSHAKE, "handshake,");
        redisNodeFlags.put(CLUSTER_NODE_NOADDR, "noaddr,");
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
                    n.port = parseInt(hostAndPort.substring(colonIdx + 1, atIdx == -1 ? hostAndPort.length() : atIdx));
                    n.cport = atIdx == -1 ? n.port + CLUSTER_PORT_INCR : parseInt(hostAndPort.substring(atIdx + 1));
                    String[] roles = list.get(2).split(",");
                    for (String role : roles) {
                        if (role.equals("myself")) {
                            server.myself = server.cluster.myself = n;
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

                    if (parseLong(list.get(4)) > 0) n.pingSent = System.currentTimeMillis();
                    if (parseLong(list.get(5)) > 0) n.pongReceived = System.currentTimeMillis();
                    n.configEpoch = parseInt(list.get(6));

                    for (int i = 8; i < list.size(); i++) {
                        int start = 0, stop = 0;
                        String argi = list.get(i);
                        if (argi.contains("-")) {
                            int idx = argi.indexOf("-");
                            start = parseInt(argi.substring(0, idx));
                            stop = parseInt(argi.substring(idx + 1));
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
            logger.info("Node configuration loaded, I'm " + server.myself.name);

            if (gossip.clusterGetMaxEpoch() > server.cluster.currentEpoch) {
                server.cluster.currentEpoch = gossip.clusterGetMaxEpoch();
            }
            return true;
        } catch (IOException e) {
            return false;
        }
    }

    public boolean clusterSaveConfig(ConfigInfo info) {
        BufferedWriter r = null;
        try {
            File file = new File(gossip.configuration.getClusterConfigfile());
            if (!file.exists()) file.createNewFile();
            r = new BufferedWriter(new FileWriter(file));
            StringBuilder ci = new StringBuilder();
            ci.append(clusterGenNodesDescription(info, CLUSTER_NODE_HANDSHAKE));
            ci.append("vars currentEpoch ").append(info.currentEpoch);
            ci.append(" lastVoteEpoch ").append(info.lastVoteEpoch);
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

    public String representClusterNodeFlags(int flags) {
        StringBuilder builder = new StringBuilder();
        if (flags == 0) {
            builder.append("noflags,");
        } else {
            redisNodeFlags.entrySet().stream().
                    filter(e -> (flags & e.getKey()) != 0).
                    forEach(e -> builder.append(e.getValue()));
        }
        builder.deleteCharAt(builder.length() - 1);
        return builder.toString();
    }

    public String clusterGenNodeDescription(NodeInfo node) {
        StringBuilder ci = new StringBuilder();

        ci.append(node.name).append(" ").append(node.ip == null ? "0.0.0.0" : node.ip).append(":").append(node.port).append("@").append(node.cport).append(" ");
        ci.append(representClusterNodeFlags(node.flags));
        if (node.slaveof != null)
            ci.append(" ").append(node.slaveof).append(" ");
        else
            ci.append(" - ");

        ci.append(node.pingSent).append(" ").append(node.pongReceived).append(" ").append(node.configEpoch).append(" ").append((node.link != null || (node.flags & CLUSTER_NODE_MYSELF) != 0) ? "connected" : "disconnected");

        int start = -1;
        for (int i = 0; i < CLUSTER_SLOTS; i++) {
            boolean bit;

            if ((bit = bitmapTestBit(node.slots, i))) {
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

        return ci.toString();
    }

    public String clusterGenNodesDescription(ConfigInfo info, int filter) {
        StringBuilder ci = new StringBuilder();
        for (NodeInfo node : info.nodes.values()) {
            if ((node.flags & filter) != 0) continue;
            ci.append(clusterGenNodeDescription(node));
            ci.append("\n");
        }
        return ci.toString();
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

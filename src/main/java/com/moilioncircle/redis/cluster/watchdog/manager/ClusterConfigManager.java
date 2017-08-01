package com.moilioncircle.redis.cluster.watchdog.manager;

import com.moilioncircle.redis.cluster.watchdog.ClusterConfigInfo;
import com.moilioncircle.redis.cluster.watchdog.ClusterConfiguration;
import com.moilioncircle.redis.cluster.watchdog.ClusterNodeInfo;
import com.moilioncircle.redis.cluster.watchdog.Version;
import com.moilioncircle.redis.cluster.watchdog.state.ClusterNode;
import com.moilioncircle.redis.cluster.watchdog.state.ServerState;
import com.moilioncircle.redis.cluster.watchdog.util.collection.ByteMap;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

import static com.moilioncircle.redis.cluster.watchdog.ClusterConstants.*;
import static com.moilioncircle.redis.cluster.watchdog.Version.PROTOCOL_V1;
import static com.moilioncircle.redis.cluster.watchdog.manager.ClusterSlotManger.bitmapTestBit;
import static com.moilioncircle.redis.cluster.watchdog.state.NodeStates.*;
import static java.lang.Integer.parseInt;
import static java.lang.Long.parseLong;
import static java.util.stream.Collectors.joining;

/**
 * @author Leon Chen
 * @since 1.0.0
 */
public class ClusterConfigManager {
    private static final Log logger = LogFactory.getLog(ClusterConfigManager.class);

    private ServerState server;
    private ClusterManagers managers;
    private ClusterConfiguration configuration;

    public ClusterConfigManager(ClusterManagers managers) {
        this.managers = managers;
        this.server = managers.server;
        this.configuration = managers.configuration;
    }

    public static Map<Byte, String> flags = new ByteMap<>();

    static {
        flags.put((byte) CLUSTER_NODE_FAIL, "fail");
        flags.put((byte) CLUSTER_NODE_PFAIL, "fail?");
        flags.put((byte) CLUSTER_NODE_SLAVE, "slave");
        flags.put((byte) CLUSTER_NODE_MYSELF, "myself");
        flags.put((byte) CLUSTER_NODE_MASTER, "master");
        flags.put((byte) CLUSTER_NODE_NOADDR, "noaddr");
        flags.put((byte) CLUSTER_NODE_HANDSHAKE, "handshake");
    }

    public boolean clusterLoadConfig() {
        String file = configuration.getClusterConfigFile();
        try (BufferedReader r = new BufferedReader(new FileReader(new File(file)))) {
            String line;
            while ((line = r.readLine()) != null) {
                List<String> args = parseLine(line);
                if (args.isEmpty()) continue;
                if (args.get(0).equals("vars")) {
                    for (int i = 1; i < args.size(); i += 2) {
                        if (args.get(i).equals("currentEpoch")) {
                            server.cluster.currentEpoch = parseInt(args.get(i + 1));
                        } else if (args.get(i).equals("lastVoteEpoch")) {
                            server.cluster.lastVoteEpoch = parseInt(args.get(i + 1));
                        } else {
                            logger.warn("Skipping unknown cluster config variable '" + args.get(i) + "'");
                        }
                    }
                } else if (args.size() < 8) {
                    throw new UnsupportedOperationException("Unrecoverable error: corrupted cluster config file.");
                } else {
                    ClusterNode node = managers.nodes.clusterLookupNode(args.get(0));
                    if (node == null) {
                        node = managers.nodes.createClusterNode(args.get(0), 0);
                        managers.nodes.clusterAddNode(node);
                    }
                    String hostAndPort = args.get(1);
                    if (!hostAndPort.contains(":")) {
                        throw new UnsupportedOperationException("Unrecoverable error: corrupted cluster config file.");
                    }
                    int cIdx = hostAndPort.indexOf(":");
                    int aIdx = hostAndPort.indexOf("@");
                    String ip = hostAndPort.substring(0, cIdx).trim();
                    node.ip = ip.equalsIgnoreCase("0.0.0.0") || ip.length() == 0 ? null : ip;
                    node.port = parseInt(hostAndPort.substring(cIdx + 1, aIdx == -1 ? hostAndPort.length() : aIdx));
                    node.busPort = aIdx == -1 ? node.port + CLUSTER_PORT_INCR : parseInt(hostAndPort.substring(aIdx + 1));

                    long now = System.currentTimeMillis();
                    for (String role : args.get(2).split(",")) {
                        switch (role) {
                            case "noflags":
                                break;
                            case "fail":
                                node.flags |= CLUSTER_NODE_FAIL;
                                node.failTime = now;
                                break;
                            case "fail?":
                                node.flags |= CLUSTER_NODE_PFAIL;
                                break;
                            case "slave":
                                node.flags |= CLUSTER_NODE_SLAVE;
                                break;
                            case "noaddr":
                                node.flags |= CLUSTER_NODE_NOADDR;
                                break;
                            case "master":
                                node.flags |= CLUSTER_NODE_MASTER;
                                break;
                            case "handshake":
                                node.flags |= CLUSTER_NODE_HANDSHAKE;
                                break;
                            case "myself":
                                node.flags |= CLUSTER_NODE_MYSELF;
                                server.myself = server.cluster.myself = node;
                                break;
                            default:
                                throw new UnsupportedOperationException("Unknown flag in redis cluster config file");
                        }
                    }

                    if (!args.get(3).equals("-")) {
                        ClusterNode master = managers.nodes.clusterLookupNode(args.get(3));
                        if (master == null) {
                            master = managers.nodes.createClusterNode(args.get(3), 0);
                            managers.nodes.clusterAddNode(master);
                        }
                        node.master = master;
                        managers.nodes.clusterNodeAddSlave(master, node);
                    }

                    if (parseLong(args.get(4)) > 0) node.pingTime = now;
                    if (parseLong(args.get(5)) > 0) node.pongTime = now;
                    node.configEpoch = parseInt(args.get(6));

                    for (int i = 8; i < args.size(); i++) {
                        int st, ed;
                        String arg = args.get(i);
                        if (arg.startsWith("[")) {
                            int idx = arg.indexOf("-");
                            char direction = arg.charAt(idx + 1);
                            int slot = parseInt(arg.substring(1, idx));
                            String name = arg.substring(idx + 3, idx + 3 + CLUSTER_NAME_LEN);
                            ClusterNode n = managers.nodes.clusterLookupNode(name);
                            if (n == null) {
                                n = managers.nodes.createClusterNode(name, 0);
                                managers.nodes.clusterAddNode(n);
                            }
                            if (direction == '>') {
                                server.cluster.migrating[slot] = n;
                            } else {
                                server.cluster.importing[slot] = n;
                            }
                            continue;
                        } else if (arg.contains("-")) {
                            int idx = arg.indexOf("-");
                            st = parseInt(arg.substring(0, idx));
                            ed = parseInt(arg.substring(idx + 1));
                        } else st = ed = parseInt(arg);
                        while (st <= ed) managers.slots.clusterAddSlot(node, st++);
                    }
                }
            }
            if (server.cluster.myself == null) {
                throw new UnsupportedOperationException("Unrecoverable error: corrupted cluster config file.");
            }
            logger.info("Node configuration loaded, I'm " + server.myself.name);

            long maxEpoch = managers.nodes.clusterGetMaxEpoch();
            server.cluster.currentEpoch = Math.max(maxEpoch, server.cluster.currentEpoch);

            for (ClusterNode node : server.cluster.nodes.values()) {
                ClusterNodeInfo info = ClusterNodeInfo.valueOf(node, server.myself);
                managers.notifyNodeAdded(info);
                if (nodePFailed(node.flags)) managers.notifyNodePFailed(info);
                if (nodeFailed(node.flags)) managers.notifyNodeFailed(info);
            }
            managers.notifyConfigChanged(ClusterConfigInfo.valueOf(server.cluster));
            return true;
        } catch (Throwable e) {
            return false;
        }
    }

    public boolean clusterSaveConfig(ClusterConfigInfo info) {
        return clusterSaveConfig(info, false);
    }

    public boolean clusterSaveConfig(ClusterConfigInfo info, boolean force) {
        BufferedWriter r = null;
        try {
            File file = new File(configuration.getClusterConfigFile());
            if (!file.exists() && !file.createNewFile()) return false;
            r = new BufferedWriter(new FileWriter(file));
            Version vs = this.configuration.getVersion();
            String d = clusterGenNodesDescription(info, CLUSTER_NODE_HANDSHAKE, vs);

            StringBuilder builder = new StringBuilder(d);
            builder.append("vars currentEpoch ").append(info.getCurrentEpoch());
            builder.append(" ").append("lastVoteEpoch ").append(info.getLastVoteEpoch());
            r.write(builder.toString()); r.flush(); if (!force) managers.notifyConfigChanged(info); return true;
        } catch (IOException e) { return false;
        } finally {
            if (r != null) try { r.close(); }
            catch (IOException e) { logger.error("unexpected IO error", e.getCause()); }
        }
    }

    public static String representClusterNodeFlags(int flags) {
        if (flags == 0) return "noflags";
        Predicate<Map.Entry<Byte, String>> t = node -> (flags & node.getKey()) != 0;
        return ClusterConfigManager.flags.entrySet().stream().filter(t).map(Map.Entry::getValue).collect(joining(","));
    }

    public static String clusterGenNodeDescription(ClusterConfigInfo info, ClusterNodeInfo node, Version v) {
        String ip = node.getIp() == null ? "0.0.0.0" : node.getIp();
        String master = node.getMaster() == null ? "-" : node.getMaster();
        long pongTime = node.getPongTime(), epoch = node.getConfigEpoch();

        StringBuilder builder = new StringBuilder(node.getName());
        builder.append(" ").append(ip).append(":").append(node.getPort());
        if (v == PROTOCOL_V1) builder.append("@").append(node.getBusPort());
        builder.append(" ").append(representClusterNodeFlags(node.getFlags()));
        builder.append(" ").append(master).append(" ").append(node.getPingTime());
        builder.append(" ").append(pongTime).append(" ").append(epoch).append(" ").append(node.getLink());

        int st = -1;
        for (int i = 0; i < CLUSTER_SLOTS; i++) {
            boolean bit = bitmapTestBit(node.getSlots(), i);
            if (bit && st == -1) st = i;
            if (st != -1 && (!bit || i == CLUSTER_SLOTS - 1)) {
                if (bit) i++;
                if (st == i - 1) builder.append(" ").append(st);
                else builder.append(" ").append(st).append("-").append(i - 1);
                st = -1;
            }
        }

        if (!nodeIsMyself(node.getFlags())) return builder.toString();

        for (int j = 0; j < CLUSTER_SLOTS; j++) {
            if (info.getMigrating()[j] != null) {
                builder.append(" [").append(j).append("->-").append(info.getMigrating()[j]).append("]");
            } else if (info.getImporting()[j] != null) {
                builder.append(" [").append(j).append("-<-").append(info.getImporting()[j]).append("]");
            }
        }
        return builder.toString();
    }

    public static String clusterGenNodesDescription(ClusterConfigInfo info, int filter, Version v) {
        StringBuilder builder = new StringBuilder();
        for (ClusterNodeInfo node : info.getNodes().values()) {
            if ((node.getFlags() & filter) != 0) continue;
            builder.append(clusterGenNodeDescription(info, node, v)).append("\n");
        }
        return builder.toString();
    }

    public static String clusterGetMessageTypeString(int type) {
        switch (type) {
            case CLUSTERMSG_TYPE_PING:
                return "ping";
            case CLUSTERMSG_TYPE_PONG:
                return "pong";
            case CLUSTERMSG_TYPE_MEET:
                return "meet";
            case CLUSTERMSG_TYPE_FAIL:
                return "fail";
            case CLUSTERMSG_TYPE_UPDATE:
                return "update";
            case CLUSTERMSG_TYPE_MFSTART:
                return "mfstart";
            case CLUSTERMSG_TYPE_PUBLISH:
                return "publish";
            case CLUSTERMSG_TYPE_FAILOVER_AUTH_ACK:
                return "auth-ack";
            case CLUSTERMSG_TYPE_FAILOVER_AUTH_REQUEST:
                return "auth-req";
            default:
                return "unknown";
        }
    }

    public static List<String> parseLine(String line) {
        List<String> args = new ArrayList<>();
        if (line.length() == 0 || line.equals("\n")) return args;
        char[] ary = line.toCharArray();
        StringBuilder s = new StringBuilder();
        boolean dq = false, q = false;
        for (int i = 0; i < ary.length; i++) {
            char c = ary[i];
            switch (c) {
                case ' ':
                    if (dq || q) s.append(' ');
                    else if (s.length() > 0) {
                        args.add(s.toString());
                        s.setLength(0);
                    }
                    break;
                case '"':
                    if (!dq && !q) {
                        dq = true;
                    } else if (q) {
                        s.append('"');
                    } else {
                        args.add(s.toString());
                        s.setLength(0);
                        dq = false;
                        if (i + 1 < ary.length && ary[i + 1] != ' ')
                            throw new UnsupportedOperationException("parse config error.");
                    }
                    break;
                case '\'':
                    if (!dq && !q) {
                        q = true;
                    } else if (dq) {
                        s.append('\'');
                    } else {
                        args.add(s.toString());
                        s.setLength(0);
                        q = false;
                        if (i + 1 < ary.length && ary[i + 1] != ' ')
                            throw new UnsupportedOperationException("parse config error.");
                    }
                    break;
                case '\\':
                    if (!dq) s.append('\\');
                    else {
                        i++;
                        if (i < ary.length) {
                            switch (ary[i]) {
                                case 'n':
                                    s.append('\n');
                                    break;
                                case 'r':
                                    s.append('\r');
                                    break;
                                case 't':
                                    s.append('\t');
                                    break;
                                case 'b':
                                    s.append('\b');
                                    break;
                                case 'f':
                                    s.append('\f');
                                    break;
                                case 'x':
                                    if (i + 2 >= ary.length) s.append("\\x");
                                    else {
                                        char high = ary[++i];
                                        char low = ary[++i];
                                        try {
                                            s.append(parseInt(new String(new char[]{high, low}), 16));
                                        } catch (Exception e) {
                                            s.append("\\x");
                                            s.append(high);
                                            s.append(low);
                                        }
                                    }
                                    break;
                                default:
                                    s.append(ary[i]);
                                    break;
                            }
                        }
                    }
                    break;
                default:
                    s.append(c);
                    break;
            }
        }
        if (dq || q) throw new UnsupportedOperationException("parse line[" + line + "] error.");
        if (s.length() > 0) args.add(s.toString());
        return args;
    }
}

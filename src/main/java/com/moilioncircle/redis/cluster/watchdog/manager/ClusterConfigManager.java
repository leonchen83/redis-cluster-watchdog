package com.moilioncircle.redis.cluster.watchdog.manager;

import com.moilioncircle.redis.cluster.watchdog.ConfigInfo;
import com.moilioncircle.redis.cluster.watchdog.NodeInfo;
import com.moilioncircle.redis.cluster.watchdog.state.ClusterNode;
import com.moilioncircle.redis.cluster.watchdog.state.ServerState;
import com.moilioncircle.redis.cluster.watchdog.util.collection.ByteMap;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.moilioncircle.redis.cluster.watchdog.ClusterConstants.*;
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

    public ClusterConfigManager(ClusterManagers managers) {
        this.managers = managers;
        this.server = managers.server;
    }

    public static Map<Byte, String> flags = new ByteMap<>();

    static {
        flags.put((byte) CLUSTER_NODE_MYSELF, "myself");
        flags.put((byte) CLUSTER_NODE_MASTER, "master");
        flags.put((byte) CLUSTER_NODE_SLAVE, "slave");
        flags.put((byte) CLUSTER_NODE_PFAIL, "fail?");
        flags.put((byte) CLUSTER_NODE_FAIL, "fail");
        flags.put((byte) CLUSTER_NODE_HANDSHAKE, "handshake");
        flags.put((byte) CLUSTER_NODE_NOADDR, "noaddr");
    }

    public boolean clusterLoadConfig() {
        String file = managers.configuration.getClusterConfigFile();
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
                    int colonIdx = hostAndPort.indexOf(":");
                    int atIdx = hostAndPort.indexOf("@");
                    node.ip = hostAndPort.substring(0, colonIdx);
                    node.port = parseInt(hostAndPort.substring(colonIdx + 1, atIdx == -1 ? hostAndPort.length() : atIdx));
                    node.busPort = atIdx == -1 ? node.port + CLUSTER_PORT_INCR : parseInt(hostAndPort.substring(atIdx + 1));
                    String[] roles = args.get(2).split(",");
                    long now = System.currentTimeMillis();
                    for (String role : roles) {
                        switch (role) {
                            case "myself":
                                server.myself = server.cluster.myself = node;
                                node.flags |= CLUSTER_NODE_MYSELF;
                                break;
                            case "master":
                                node.flags |= CLUSTER_NODE_MASTER;
                                break;
                            case "slave":
                                node.flags |= CLUSTER_NODE_SLAVE;
                                break;
                            case "fail?":
                                node.flags |= CLUSTER_NODE_PFAIL;
                                break;
                            case "fail":
                                node.flags |= CLUSTER_NODE_FAIL;
                                node.failTime = now;
                                break;
                            case "handshake":
                                node.flags |= CLUSTER_NODE_HANDSHAKE;
                                break;
                            case "noaddr":
                                node.flags |= CLUSTER_NODE_NOADDR;
                                break;
                            case "noflags":
                                break;
                            default:
                                throw new UnsupportedOperationException("Unknown flag in redis cluster config file");
                        }
                    }

                    ClusterNode master;
                    if (!args.get(3).equals("-")) {
                        master = managers.nodes.clusterLookupNode(args.get(3));
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
                                server.cluster.migratingSlotsTo[slot] = n;
                            } else {
                                server.cluster.importingSlotsFrom[slot] = n;
                            }
                            continue;
                        } else if (arg.contains("-")) {
                            int idx = arg.indexOf("-");
                            st = parseInt(arg.substring(0, idx));
                            ed = parseInt(arg.substring(idx + 1));
                        } else {
                            st = ed = parseInt(arg);
                        }
                        while (st <= ed) managers.slots.clusterAddSlot(node, st++);
                    }
                }
            }
            if (server.cluster.myself == null) {
                throw new UnsupportedOperationException("Unrecoverable error: corrupted cluster config file.");
            }
            logger.info("Node configuration loaded, I'm " + server.myself.name);

            long maxEpoch = managers.nodes.clusterGetMaxEpoch();
            if (maxEpoch > server.cluster.currentEpoch) {
                server.cluster.currentEpoch = maxEpoch;
            }
            return true;
        } catch (Throwable e) {
            return false;
        }
    }

    public boolean clusterSaveConfig(ConfigInfo info) {
        BufferedWriter r = null;
        try {
            File file = new File(managers.configuration.getClusterConfigFile());
            if (!file.exists() && !file.createNewFile()) return false;
            r = new BufferedWriter(new FileWriter(file));
            String line = clusterGenNodesDescription(info, CLUSTER_NODE_HANDSHAKE) +
                    "vars currentEpoch " + info.currentEpoch +
                    " lastVoteEpoch " + info.lastVoteEpoch;
            r.write(line);
            r.flush();
            return true;
        } catch (IOException e) {
            return false;
        } finally {
            if (r != null) try {
                r.close();
            } catch (IOException e) {
                logger.error("unexpected IO error", e.getCause());
            }
        }
    }

    public static String representClusterNodeFlags(int flags) {
        if (flags == 0) return "noflags";
        return ClusterConfigManager.flags.entrySet().stream().
                filter(e -> (flags & e.getKey()) != 0).
                map(Map.Entry::getValue).collect(joining(","));
    }

    public String clusterGenNodeDescription(NodeInfo node) {
        StringBuilder builder = new StringBuilder();

        builder.append(node.name).append(" ").append(node.ip == null ? "0.0.0.0" : node.ip);
        builder.append(":").append(node.port).append("@").append(node.busPort).append(" ");
        builder.append(representClusterNodeFlags(node.flags)).append(" ");
        builder.append(node.master == null ? "-" : node.master).append(" ");
        builder.append(node.pingTime).append(" ").append(node.pongTime);
        builder.append(" ").append(node.configEpoch).append(" ");
        builder.append((node.link != null || (node.flags & CLUSTER_NODE_MYSELF) != 0) ? "connected" : "disconnected");

        int st = -1;
        for (int i = 0; i < CLUSTER_SLOTS; i++) {
            boolean bit;

            if ((bit = ClusterSlotManger.bitmapTestBit(node.slots, i)) && st == -1) {
                st = i;
            }
            if (st != -1 && (!bit || i == CLUSTER_SLOTS - 1)) {
                if (bit) i++;
                if (st == i - 1) {
                    builder.append(" ").append(st);
                } else {
                    builder.append(" ").append(st).append("-").append(i - 1);
                }
                st = -1;
            }
        }

        if ((node.flags & CLUSTER_NODE_MYSELF) != 0) {
            for (int j = 0; j < CLUSTER_SLOTS; j++) {
                if (server.cluster.migratingSlotsTo[j] != null) {
                    builder.append(" [").append(j).append("->-").append(server.cluster.migratingSlotsTo[j].name).append("]");
                } else if (server.cluster.importingSlotsFrom[j] != null) {
                    builder.append(" [").append(j).append("-<-").append(server.cluster.importingSlotsFrom[j].name).append("]");
                }
            }
        }

        return builder.toString();
    }

    public String clusterGenNodesDescription(ConfigInfo info, int filter) {
        StringBuilder builder = new StringBuilder();
        for (NodeInfo node : info.nodes.values()) {
            if ((node.flags & filter) != 0) continue;
            builder.append(clusterGenNodeDescription(node));
            builder.append("\n");
        }
        return builder.toString();
    }

    public String clusterGetMessageTypeString(int type) {
        switch (type) {
            case CLUSTERMSG_TYPE_PING:
                return "pingTime";
            case CLUSTERMSG_TYPE_PONG:
                return "pongTime";
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
                            throw new UnsupportedOperationException("parse file error.");
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
                            throw new UnsupportedOperationException("parse file error.");
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

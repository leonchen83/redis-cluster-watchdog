package com.moilioncircle.redis.cluster.watchdog.manager;

import com.moilioncircle.redis.cluster.watchdog.config.ConfigInfo;
import com.moilioncircle.redis.cluster.watchdog.config.NodeInfo;
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
 * Created by Baoyi Chen on 2017/7/12.
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
        String fileName = managers.configuration.getClusterConfigFile();
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
                } else if (list.size() < 8) {
                    throw new UnsupportedOperationException("Unrecoverable error: corrupted cluster config file.");
                } else {
                    ClusterNode n = managers.nodes.clusterLookupNode(list.get(0));
                    if (n == null) {
                        n = managers.nodes.createClusterNode(list.get(0), 0);
                        managers.nodes.clusterAddNode(n);
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
                        switch (role) {
                            case "myself":
                                server.myself = server.cluster.myself = n;
                                n.flags |= CLUSTER_NODE_MYSELF;
                                break;
                            case "master":
                                n.flags |= CLUSTER_NODE_MASTER;
                                break;
                            case "slave":
                                n.flags |= CLUSTER_NODE_SLAVE;
                                break;
                            case "fail?":
                                n.flags |= CLUSTER_NODE_PFAIL;
                                break;
                            case "fail":
                                n.flags |= CLUSTER_NODE_FAIL;
                                n.failTime = System.currentTimeMillis();
                                break;
                            case "handshake":
                                n.flags |= CLUSTER_NODE_HANDSHAKE;
                                break;
                            case "noaddr":
                                n.flags |= CLUSTER_NODE_NOADDR;
                                break;
                            case "noflags":
                                // NOP
                                break;
                            default:
                                throw new UnsupportedOperationException("Unknown flag in redis cluster config file");
                        }
                    }

                    ClusterNode master;
                    if (!list.get(3).equals("-")) {
                        master = managers.nodes.clusterLookupNode(list.get(3));
                        if (master == null) {
                            master = managers.nodes.createClusterNode(list.get(3), 0);
                            managers.nodes.clusterAddNode(master);
                        }
                        n.slaveof = master;
                        managers.nodes.clusterNodeAddSlave(master, n);
                    }

                    if (parseLong(list.get(4)) > 0) n.pingSent = System.currentTimeMillis();
                    if (parseLong(list.get(5)) > 0) n.pongReceived = System.currentTimeMillis();
                    n.configEpoch = parseInt(list.get(6));

                    for (int i = 8; i < list.size(); i++) {
                        int start, stop;
                        String argi = list.get(i);
                        if (argi.contains("-")) {
                            int idx = argi.indexOf("-");
                            start = parseInt(argi.substring(0, idx));
                            stop = parseInt(argi.substring(idx + 1));
                        } else {
                            start = stop = parseInt(argi);
                        }
                        while (start <= stop) managers.slots.clusterAddSlot(n, start++);
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
            String ci = clusterGenNodesDescription(info, CLUSTER_NODE_HANDSHAKE) +
                    "vars currentEpoch " + info.currentEpoch +
                    " lastVoteEpoch " + info.lastVoteEpoch;
            r.write(ci);
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

            if ((bit = ClusterSlotManger.bitmapTestBit(node.slots, i))) {
                if (start == -1) start = i;
            }
            if (start != -1 && (!bit || i == CLUSTER_SLOTS - 1)) {
                if (bit) i++;
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

    public static List<String> parseLine(String line) {
        char[] ary = line.toCharArray();
        List<String> list = new ArrayList<>();
        StringBuilder s = new StringBuilder();
        boolean inq = false, insq = false;
        for (int i = 0; i < ary.length; i++) {
            char c = ary[i];
            switch (c) {
                case ' ':
                    if (inq || insq) s.append(' ');
                    else if (s.length() > 0) {
                        list.add(s.toString());
                        s.setLength(0);
                    }
                    break;
                case '"':
                    if (!inq && !insq) {
                        inq = true;
                    } else if (insq) {
                        s.append('"');
                    } else {
                        list.add(s.toString());
                        s.setLength(0);
                        inq = false;
                        if (i + 1 < ary.length && ary[i + 1] != ' ')
                            throw new UnsupportedOperationException("parse file error.");
                    }
                    break;
                case '\'':
                    if (!inq && !insq) {
                        insq = true;
                    } else if (inq) {
                        s.append('\'');
                    } else {
                        list.add(s.toString());
                        s.setLength(0);
                        insq = false;
                        if (i + 1 < ary.length && ary[i + 1] != ' ')
                            throw new UnsupportedOperationException("parse file error.");
                    }
                    break;
                case '\\':
                    if (!inq) s.append('\\');
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
        if (inq || insq) throw new UnsupportedOperationException("parse line[" + line + "] error.");
        if (s.length() > 0) list.add(s.toString());
        return list;
    }
}

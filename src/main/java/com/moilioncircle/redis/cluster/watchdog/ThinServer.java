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

package com.moilioncircle.redis.cluster.watchdog;

import com.moilioncircle.redis.cluster.watchdog.codec.RedisDecoder;
import com.moilioncircle.redis.cluster.watchdog.codec.RedisEncoder;
import com.moilioncircle.redis.cluster.watchdog.config.ConfigInfo;
import com.moilioncircle.redis.cluster.watchdog.config.NodeInfo;
import com.moilioncircle.redis.cluster.watchdog.manager.ClusterManagers;
import com.moilioncircle.redis.cluster.watchdog.state.ClusterNode;
import com.moilioncircle.redis.cluster.watchdog.state.ServerState;
import com.moilioncircle.redis.cluster.watchdog.util.Arrays;
import com.moilioncircle.redis.cluster.watchdog.util.net.NioBootstrapConfiguration;
import com.moilioncircle.redis.cluster.watchdog.util.net.NioBootstrapImpl;
import com.moilioncircle.redis.cluster.watchdog.util.net.transport.Transport;
import com.moilioncircle.redis.cluster.watchdog.util.net.transport.TransportListener;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static com.moilioncircle.redis.cluster.watchdog.ClusterConstants.*;
import static com.moilioncircle.redis.cluster.watchdog.config.ConfigInfo.valueOf;
import static com.moilioncircle.redis.cluster.watchdog.manager.ClusterNodeManager.getRandomHexChars;
import static com.moilioncircle.redis.cluster.watchdog.manager.ClusterSlotManger.bitmapTestBit;
import static com.moilioncircle.redis.cluster.watchdog.manager.ClusterSlotManger.keyHashSlot;
import static com.moilioncircle.redis.cluster.watchdog.state.States.*;
import static java.lang.Integer.parseInt;
import static java.lang.Long.parseLong;

/**
 * @author Leon Chen
 * @since 2.1.0
 */
public class ThinServer {
    private static final Log logger = LogFactory.getLog(ThinServer.class);
    private ServerState server;
    private ClusterManagers managers;

    public ThinServer(ClusterManagers managers) {
        this.managers = managers;
        this.server = managers.server;
    }

    public void start() {
        NioBootstrapImpl<Object> cfd = new NioBootstrapImpl<>(true, new NioBootstrapConfiguration());
        cfd.setEncoder(RedisEncoder::new);
        cfd.setDecoder(RedisDecoder::new);
        cfd.setup();
        cfd.setTransportListener(new TransportListener<Object>() {
            @Override
            public void onConnected(Transport<Object> transport) {
                logger.info("[acceptor] > " + transport.toString());
            }

            @Override
            public void onMessage(Transport<Object> transport, Object message) {
                managers.executor.execute(() -> {
                    ConfigInfo oldInfo = valueOf(managers.server.cluster);
                    clusterCommand(transport, (byte[][]) message);
                    ConfigInfo newInfo = valueOf(managers.server.cluster);
                    if (!oldInfo.equals(newInfo))
                        managers.file.submit(() -> managers.configs.clusterSaveConfig(newInfo));
                });
            }

            @Override
            public void onDisconnected(Transport<Object> transport, Throwable cause) {
                logger.info("[acceptor] < " + transport.toString());
            }
        });
        try {
            cfd.connect(null, managers.configuration.getClusterAnnouncePort()).get();
        } catch (InterruptedException | ExecutionException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            } else {
                throw new UnsupportedOperationException(e.getCause());
            }
        }
    }

    public void clusterCommand(Transport<Object> t, byte[][] message) {
        String arg0 = new String(message[0]);
        if (!arg0.equalsIgnoreCase("cluster")) {
            t.write(("-ERR Unsupported operation " + Arrays.deepToString(message) + "\r\n").getBytes(), true);
            return;
        }
        // cluster commands
        String[] argv = new String[message.length];
        for (int i = 0; i < message.length; i++) {
            argv[i] = new String(message[i]);
        }
        if (argv[1].equalsIgnoreCase("meet") && (argv.length == 4 || argv.length == 5)) {
            int cport;
            int port = parseInt(argv[3]);
            if (argv.length == 5) {
                cport = parseInt(argv[4]);
            } else {
                cport = port + CLUSTER_PORT_INCR;
            }

            if (managers.nodes.clusterStartHandshake(argv[2], port, cport)) {
                t.write("+OK\r\n".getBytes(), true);
            } else {
                t.write(("-ERR Invalid node address specified:" + argv[2] + ":" + argv[3] + "\r\n").getBytes(), true);
            }
        } else if (argv[1].equalsIgnoreCase("nodes") && argv.length == 2) {
            /* CLUSTER NODES */
            String ci = managers.configs.clusterGenNodesDescription(valueOf(server.cluster), 0);
            t.write(("$" + ci.length() + "\r\n" + ci + "\r\n").getBytes(), true);
        } else if (argv[1].equalsIgnoreCase("myid") && argv.length == 2) {
            /* CLUSTER MYID */
            t.write(("+" + server.myself.name + "\r\n").getBytes(), true);
        } else if (argv[1].equalsIgnoreCase("slots") && argv.length == 2) {
            t.write(clusterReplyMultiBulkSlots().getBytes(), true);
        } else if (argv[1].equalsIgnoreCase("flushslots") && argv.length == 2) {
            /* CLUSTER FLUSHSLOTS */
            managers.slots.clusterDelNodeSlots(server.myself);
            managers.states.clusterUpdateState();
            t.write(("+OK\r\n").getBytes(), true);
        } else if ((argv[1].equalsIgnoreCase("addslots") || argv[1].equalsIgnoreCase("delslots")) && argv.length >= 3) {
//            /* CLUSTER ADDSLOTS <slot> [slot] ... */
//            /* CLUSTER DELSLOTS <slot> [slot] ... */
//            byte[] slots = new byte[CLUSTER_SLOTS];
//            boolean del = argv[1].equalsIgnoreCase("delslots");
//
//            for (int i = 2; i < argv.length; i++) {
//                int slot = parseInt(argv[i]);
//
//                if (del && server.cluster.slots[slot] == null) {
//                    t.write(("-ERR Slot " + slot + " is already unassigned\r\n").getBytes(), true);
//                    return;
//                } else if (!del && server.cluster.slots[slot] != null) {
//                    t.write(("-ERR Slot " + slot + " is already busy\r\n").getBytes(), true);
//                    return;
//                }
//                if (slots[slot]++ == 1) {
//                    t.write(("-ERR Slot " + slot + " specified multiple times\r\n").getBytes(), true);
//                    return;
//                }
//            }
//            for (int i = 0; i < CLUSTER_SLOTS; i++) {
//                if (slots[i] != 0) {
//                    if (server.cluster.importingSlotsFrom[i] != null)
//                        server.cluster.importingSlotsFrom[i] = null;
//                    if (del) managers.slots.clusterDelSlot(i);
//                    else managers.slots.clusterAddSlot(managers.server.myself, i);
//                }
//            }
//            managers.clusterUpdateState();
//            t.write(("+OK\r\n").getBytes(), true);
            t.write(("-ERR Unsupported operation [cluster " + argv[1] + "]\r\n").getBytes(), true);
        } else if (argv[1].equalsIgnoreCase("setslot") && argv.length >= 4) {
//            /* SETSLOT 10 MIGRATING <node ID> */
//            /* SETSLOT 10 IMPORTING <node ID> */
//            /* SETSLOT 10 STABLE */
//            /* SETSLOT 10 NODE <node ID> */
//
//            if (nodeIsSlave(server.myself)) {
//                t.write("-ERR Please use SETSLOT only with masters.\r\n".getBytes(), true);
//                return;
//            }
//
//            int slot = parseInt(argv[2]);
//
//            if (argv[3].equalsIgnoreCase("migrating") && argv.length == 5) {
//                if (server.cluster.slots[slot] == null || !server.cluster.slots[slot].equals(server.myself)) {
//                    t.write(("-ERR I'm not the owner of hash slot " + slot + "\r\n").getBytes(), true);
//                    return;
//                }
//                ClusterNode n = managers.nodes.clusterLookupNode(argv[4]);
//                if (n == null) {
//                    t.write(("-ERR I don't know about node " + argv[4] + "\r\n").getBytes(), true);
//                    return;
//                }
//                server.cluster.migratingSlotsTo[slot] = n;
//            } else if (argv[3].equalsIgnoreCase("importing") && argv.length == 5) {
//                if (server.cluster.slots[slot] != null && server.cluster.slots[slot].equals(server.myself)) {
//                    t.write(("-ERR I'm already the owner of hash slot " + slot + "\r\n").getBytes(), true);
//                    return;
//                }
//                ClusterNode n = managers.nodes.clusterLookupNode(argv[4]);
//                if (n == null) {
//                    t.write(("-ERR I don't know about node " + argv[4] + "\r\n").getBytes(), true);
//                    return;
//                }
//                server.cluster.importingSlotsFrom[slot] = n;
//            } else if (argv[3].equalsIgnoreCase("stable") && argv.length == 4) {
//                /* CLUSTER SETSLOT <SLOT> STABLE */
//                server.cluster.importingSlotsFrom[slot] = null;
//                server.cluster.migratingSlotsTo[slot] = null;
//            } else if (argv[3].equalsIgnoreCase("node") && argv.length == 5) {
//                /* CLUSTER SETSLOT <SLOT> NODE <NODE ID> */
//                ClusterNode n = managers.nodes.clusterLookupNode(argv[4]);
//
//                if (n == null) {
//                    t.write(("-ERR Unknown node " + argv[4] + "\r\n").getBytes(), true);
//                    return;
//                }
//
//                if (server.cluster.migratingSlotsTo[slot] != null)
//                    server.cluster.migratingSlotsTo[slot] = null;
//
//                if (n.equals(server.myself) && server.cluster.importingSlotsFrom[slot] != null) {
//                    if (clusterBumpConfigEpochWithoutConsensus()) {
//                        logger.warn("configEpoch updated after importing slot " + slot);
//                    }
//                    server.cluster.importingSlotsFrom[slot] = null;
//                }
//                managers.slots.clusterDelSlot(slot);
//                managers.slots.clusterAddSlot(n, slot);
//            } else {
//                t.write("-ERR Invalid CLUSTER SETSLOT action or number of arguments\r\n".getBytes(), true);
//                return;
//            }
//            managers.clusterUpdateState();
//            t.write(("+OK\r\n").getBytes(), true);
            t.write(("-ERR Unsupported operation [cluster " + argv[1] + "]\r\n").getBytes(), true);
        } else if (argv[1].equalsIgnoreCase("bumpepoch") && argv.length == 2) {
            boolean retval = clusterBumpConfigEpochWithoutConsensus();
            String reply = "+" + (retval ? "BUMPED" : "STILL") + " " + server.myself.configEpoch + "\r\n";
            t.write(reply.getBytes(), true);
        } else if (argv[1].equalsIgnoreCase("info") && argv.length == 2) {
            String[] statestr = {"ok", "fail", "needhelp"};
            int slotsAssigned = 0, slotsOk = 0, slotsFail = 0, slotsPfail = 0;

            for (int j = 0; j < CLUSTER_SLOTS; j++) {
                ClusterNode n = server.cluster.slots[j];

                if (n == null) continue;
                slotsAssigned++;
                if (nodeFailed(n)) {
                    slotsFail++;
                } else if (nodePFailed(n)) {
                    slotsPfail++;
                } else {
                    slotsOk++;
                }
            }

            long myepoch = (nodeIsSlave(server.myself) && server.myself.slaveof != null) ? server.myself.slaveof.configEpoch : server.myself.configEpoch;

            StringBuilder info = new StringBuilder("cluster_state:").append(statestr[server.cluster.state]).append("\r\n")
                    .append("cluster_slots_assigned:").append(slotsAssigned).append("\r\n")
                    .append("cluster_slots_ok:").append(slotsOk).append("\r\n")
                    .append("cluster_slots_pfail:").append(slotsPfail).append("\r\n")
                    .append("cluster_slots_fail:").append(slotsFail).append("\r\n")
                    .append("cluster_known_nodes:").append(server.cluster.nodes.size()).append("\r\n")
                    .append("cluster_size:").append(server.cluster.size).append("\r\n")
                    .append("cluster_current_epoch:").append(server.cluster.currentEpoch).append("\r\n")
                    .append("cluster_my_epoch:").append(myepoch).append("\r\n");


            long totMsgSent = 0;
            long totMsgReceived = 0;

            for (int i = 0; i < CLUSTERMSG_TYPE_COUNT; i++) {
                if (server.cluster.statsBusMessagesSent[i] == 0) continue;
                totMsgSent += server.cluster.statsBusMessagesSent[i];
                info.append("cluster_stats_messages_").append(managers.configs.clusterGetMessageTypeString(i)).append("_sent:").append(server.cluster.statsBusMessagesSent[i]).append("\r\n");
            }

            info.append("cluster_stats_messages_sent:").append(totMsgSent).append("\r\n");

            for (int i = 0; i < CLUSTERMSG_TYPE_COUNT; i++) {
                if (server.cluster.statsBusMessagesReceived[i] == 0) continue;
                totMsgReceived += server.cluster.statsBusMessagesReceived[i];
                info.append("cluster_stats_messages_").append(managers.configs.clusterGetMessageTypeString(i)).append("_received:").append(server.cluster.statsBusMessagesReceived[i]).append("\r\n");
            }

            info.append("cluster_stats_messages_received:").append(totMsgReceived).append("\r\n");
            t.write(("$" + info.length() + "\r\n" + info.toString() + "\r\n").getBytes(), true);
        } else if (argv[1].equalsIgnoreCase("saveconfig") && argv.length == 2) {
            if (!managers.configs.clusterSaveConfig(valueOf(server.cluster))) {
                t.write(("-ERR Error saving the cluster node config\r\n").getBytes(), true);
            }
            t.write("+OK\r\n".getBytes(), true);
        } else if (argv[1].equalsIgnoreCase("keyslot") && argv.length == 3) {
            t.write((":" + String.valueOf(keyHashSlot(argv[2])) + "\r\n").getBytes(), true);
        } else if (argv[1].equalsIgnoreCase("countkeysinslot") && argv.length == 3) {
            t.write(("-ERR Unsupported operation [cluster " + argv[1] + "]\r\n").getBytes(), true);
        } else if (argv[1].equalsIgnoreCase("forget") && argv.length == 3) {
            ClusterNode n = managers.nodes.clusterLookupNode(argv[2]);

            if (n == null) {
                t.write(("-ERR Unknown node " + argv[2] + "\r\n").getBytes(), true);
                return;
            } else if (n.equals(server.myself)) {
                t.write(("-ERR I tried hard but I can't forget myself...\r\n").getBytes(), true);
                return;
            } else if (nodeIsSlave(server.myself) && server.myself.slaveof.equals(n)) {
                t.write(("-ERR Can't forget my master!\r\n").getBytes(), true);
                return;
            }
            managers.blacklists.clusterBlacklistAddNode(n);
            managers.nodes.clusterDelNode(n);
            managers.states.clusterUpdateState();
            t.write("+OK\r\n".getBytes(), true);
        } else if (argv[1].equalsIgnoreCase("replicate") && argv.length == 3) {
            ClusterNode n = managers.nodes.clusterLookupNode(argv[2]);

            if (n == null) {
                t.write(("-ERR Unknown node " + argv[2] + "\r\n").getBytes(), true);
                return;
            }

            if (n.equals(server.myself)) {
                t.write(("-ERR Can't replicate myself\r\n").getBytes(), true);
                return;
            }

            if (nodeIsSlave(n)) {
                t.write(("-ERR I can only replicate a master, not a slave.\r\n").getBytes(), true);
                return;
            }

            if (nodeIsMaster(server.myself) && (server.myself.numslots != 0)) {
                t.write(("-ERR To set a master the node must be empty and without assigned slots.\r\n").getBytes(), true);
                return;
            }

            managers.nodes.clusterSetMyMaster(n);
            managers.states.clusterUpdateState();
            t.write("+OK\r\n".getBytes(), true);
        } else if (argv[1].equalsIgnoreCase("slaves") && argv.length == 3) {
            ClusterNode n = managers.nodes.clusterLookupNode(argv[2]);

            if (n == null) {
                t.write(("-ERR Unknown node " + argv[2] + "\r\n").getBytes(), true);
                return;
            }

            if (nodeIsSlave(n)) {
                t.write(("-ERR The specified node is not a master\r\n").getBytes(), true);
                return;
            }

            StringBuilder ci = new StringBuilder();
            for (int j = 0; j < n.numslaves; j++) {
                ci.append(managers.configs.clusterGenNodeDescription(NodeInfo.valueOf(n.slaves.get(j), server.cluster.myself)));
            }
            t.write(("$" + ci.length() + "\r\n" + ci.toString() + "\r\n").getBytes(), true);
        } else if (argv[1].equalsIgnoreCase("count-failure-reports") && argv.length == 3) {
            /* CLUSTER COUNT-FAILURE-REPORTS <NODE ID> */
            ClusterNode n = managers.nodes.clusterLookupNode(argv[2]);

            if (n == null) {
                t.write(("-ERR Unknown node " + argv[2] + "\r\n").getBytes(), true);
            } else {
                t.write((":" + String.valueOf(managers.nodes.clusterNodeFailureReportsCount(n)) + "\r\n").getBytes(), true);
            }
        } else if (argv[1].equalsIgnoreCase("set-config-epoch") && argv.length == 3) {
            long epoch = parseLong(argv[2]);

            if (epoch < 0) {
                t.write(("-ERR Invalid config epoch specified: " + epoch + "\r\n").getBytes(), true);
            } else if (server.cluster.nodes.size() > 1) {
                t.write(("-ERR The user can assign a config epoch only when the node does not know any other node.\r\n").getBytes(), true);
            } else if (server.myself.configEpoch != 0) {
                t.write(("-ERR Node config epoch is already non-zero\r\n").getBytes(), true);
            } else {
                server.myself.configEpoch = epoch;
                logger.info("configEpoch set to " + server.myself.configEpoch + " via CLUSTER SET-CONFIG-EPOCH");
                if (server.cluster.currentEpoch < epoch)
                    server.cluster.currentEpoch = epoch;
                managers.states.clusterUpdateState();
                t.write("+OK\r\n".getBytes(), true);
            }
        } else if (argv[1].equalsIgnoreCase("reset") && (argv.length == 2 || argv.length == 3)) {
            boolean hard = false;
            if (argv.length == 3) {
                if (argv[2].equalsIgnoreCase("hard")) hard = true;
                else if (argv[2].equalsIgnoreCase("soft")) hard = false;
                else t.write("-ERR Syntax error.\r\n".getBytes(), true);
            }
            clusterReset(hard);
            t.write(("+OK\r\n").getBytes(), true);
        } else {
            t.write(("-ERR Wrong CLUSTER subcommand or number of arguments\r\n").getBytes(), true);
        }
    }

    public boolean clusterBumpConfigEpochWithoutConsensus() {
        long maxEpoch = managers.nodes.clusterGetMaxEpoch();
        if (server.myself.configEpoch == 0 || server.myself.configEpoch != maxEpoch) {
            server.cluster.currentEpoch++;
            server.myself.configEpoch = server.cluster.currentEpoch;
            logger.info("New configEpoch set to " + server.myself.configEpoch);
            return true;
        }
        return false;
    }

    public String clusterReplyMultiBulkSlots() {
        int numMasters = 0;
        StringBuilder ci = new StringBuilder();
        for (ClusterNode node : server.cluster.nodes.values()) {
            int start = -1;
            if (!nodeIsMaster(node) || node.numslots == 0) continue;

            for (int i = 0; i < CLUSTER_SLOTS; i++) {
                boolean bit;
                if ((bit = bitmapTestBit(node.slots, i))) {
                    if (start == -1) start = i;
                }
                if (start != -1 && (!bit || i == CLUSTER_SLOTS - 1)) {
                    StringBuilder builder = new StringBuilder();
                    int nestedElements = 3;
                    if (bit) i++;
                    if (start == i - 1) {
                        builder.append(":").append(start).append("\r\n");
                        builder.append(":").append(start).append("\r\n");
                    } else {
                        builder.append(":").append(start).append("\r\n");
                        builder.append(":").append(i - 1).append("\r\n");
                    }
                    start = -1;
                    builder.append("*3\r\n");
                    builder.append("$").append(node.ip.length()).append("\r\n").append(node.ip).append("\r\n");
                    builder.append(":").append(node.port).append("\r\n");
                    builder.append("$").append(node.name.length()).append("\r\n").append(node.name).append("\r\n");
                    for (int j = 0; j < node.numslaves; j++) {
                        if (nodeFailed(node.slaves.get(j))) continue;
                        ClusterNode n = node.slaves.get(j);
                        builder.append("*3\r\n");
                        builder.append("$").append(n.ip.length()).append("\r\n").append(n.ip).append("\r\n");
                        builder.append(":").append(n.port).append("\r\n");
                        builder.append("$").append(n.name.length()).append("\r\n").append(n.name).append("\r\n");
                        nestedElements++;
                    }
                    builder.insert(0, "*" + nestedElements + "\r\n");
                    ci.append(builder.toString());
                    numMasters++;
                }
            }
        }
        ci.insert(0, "*" + numMasters + "\r\n");
        return ci.toString();
    }

    public void clusterReset(boolean hard) {
        if (nodeIsSlave(server.myself)) {
            managers.nodes.clusterSetNodeAsMaster(server.myself);
            managers.replications.replicationUnsetMaster();
        }

        for (int i = 0; i < CLUSTER_SLOTS; i++)
            managers.slots.clusterDelSlot(i);

        List<ClusterNode> nodes = new ArrayList<>(server.cluster.nodes.values());
        for (ClusterNode node : nodes) {
            if (node.equals(server.myself)) continue;
            managers.nodes.clusterDelNode(node);
        }
        if (!hard) return;

        server.cluster.currentEpoch = 0;
        server.cluster.lastVoteEpoch = 0;
        server.myself.configEpoch = 0;
        logger.info("configEpoch set to 0 via CLUSTER RESET HARD");
        String old = server.myself.name;
        server.cluster.nodes.remove(old);
        server.myself.name = getRandomHexChars();
        managers.nodes.clusterAddNode(server.myself);
        logger.info("Node hard reset, now I'm " + server.myself.name);
    }
}

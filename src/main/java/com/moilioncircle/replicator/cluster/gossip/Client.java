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

import com.moilioncircle.replicator.cluster.ClusterNode;
import com.moilioncircle.replicator.cluster.util.net.transport.Transport;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import static com.moilioncircle.replicator.cluster.ClusterConstants.*;
import static java.lang.Integer.parseInt;
import static java.lang.Long.parseLong;

/**
 * @author Leon Chen
 * @since 2.1.0
 */
public class Client {
    private static final Log logger = LogFactory.getLog(Client.class);
    private Server server;
    private ThinGossip gossip;

    public Client(ThinGossip gossip) {
        this.gossip = gossip;
        this.server = gossip.server;
    }

    public void clusterCommand(Transport t, String[] argv) {
        if (!gossip.configuration.isClusterEnabled()) {
            reply(t, "This instance has cluster support disabled");
            return;
        }

        if (argv[1].equalsIgnoreCase("meet") && (argv.length == 4 || argv.length == 5)) {
            /* CLUSTER MEET <ip> <port> [cport] */
            int cport = 0;
            int port = parseInt(argv[3]);
            if (argv.length == 5) {
                cport = parseInt(argv[4]);
            } else {
                cport = port + CLUSTER_PORT_INCR;
            }

            gossip.clusterStartHandshake(argv[2], port, cport);
        } else if (argv[1].equalsIgnoreCase("nodes") && argv.length == 2) {
            /* CLUSTER NODES */
            String ci = gossip.configManager.clusterGenNodesDescription(0);
            reply(t, ci);
        } else if (argv[1].equalsIgnoreCase("myid") && argv.length == 2) {
            /* CLUSTER MYID */
            reply(t, server.myself.name);
        } else if (argv[1].equalsIgnoreCase("flushslots") && argv.length == 2) {
            //unsupported
        } else if ((argv[1].equalsIgnoreCase("addslots") || argv[1].equalsIgnoreCase("delslots")) && argv.length >= 3) {
            //unsupported
        } else if (argv[1].equalsIgnoreCase("setslot") && argv.length >= 4) {
            //unsupported
        } else if (argv[1].equalsIgnoreCase("bumpepoch") && argv.length == 2) {
            /* CLUSTER BUMPEPOCH */
            boolean retval = clusterBumpConfigEpochWithoutConsensus();
            String reply = new StringBuilder("+").append(retval ? "BUMPED" : "STILL").append(" ").append(server.myself.configEpoch).append("\r\n").toString();
            replyString(t, reply);
        } else if (argv[1].equalsIgnoreCase("info") && argv.length == 2) {
            /* CLUSTER INFO */
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
                info.append("cluster_stats_messages_" + gossip.configManager.clusterGetMessageTypeString(i) + "_sent:").append(server.cluster.statsBusMessagesSent[i]).append("\r\n");
            }

            info.append("cluster_stats_messages_sent:").append(totMsgSent).append("\r\n");

            for (int i = 0; i < CLUSTERMSG_TYPE_COUNT; i++) {
                if (server.cluster.statsBusMessagesReceived[i] == 0) continue;
                totMsgReceived += server.cluster.statsBusMessagesReceived[i];
                info.append("cluster_stats_messages_" + gossip.configManager.clusterGetMessageTypeString(i) + "_received:").append(server.cluster.statsBusMessagesReceived[i]).append("\r\n");
            }

            info.append("cluster_stats_messages_received:").append(totMsgReceived).append("\r\n");

            String s = "$" + info.length() + "\r\n" + info.toString() + "\r\n";
            replyString(t, s);
        } else if (argv[1].equalsIgnoreCase("saveconfig") && argv.length == 2) {
            if (gossip.configManager.clusterSaveConfig())
                reply(t, "OK");
            else
                replyError(t, "error saving the cluster node config.");
        } else if (argv[1].equalsIgnoreCase("keyslot") && argv.length == 3) {
            /* CLUSTER KEYSLOT <key> */
            reply(t, String.valueOf(gossip.slotManger.keyHashSlot(argv[2])));
        } else if (argv[1].equalsIgnoreCase("countkeysinslot") && argv.length == 3) {
            //unsupported
        } else if (argv[1].equalsIgnoreCase("forget") && argv.length == 3) {
            /* CLUSTER FORGET <NODE ID> */
            ClusterNode n = gossip.nodeManager.clusterLookupNode(argv[2]);

            if (n == null) {
                replyError(t, "Unknown node " + argv[2]);
                return;
            } else if (n.equals(server.myself)) {
                replyError(t, "I tried hard but I can't forget myself...");
                return;
            } else if (nodeIsSlave(server.myself) && server.myself.slaveof.equals(n)) {
                replyError(t, "Can't forget my master!");
                return;
            }
            gossip.blacklistManager.clusterBlacklistAddNode(n);
            gossip.nodeManager.clusterDelNode(n);
            gossip.clusterDoBeforeSleep(CLUSTER_TODO_UPDATE_STATE | CLUSTER_TODO_SAVE_CONFIG);
            reply(t, "OK");
        } else if (argv[1].equalsIgnoreCase("replicate") && argv.length == 3) {
            /* CLUSTER REPLICATE <NODE ID> */
            ClusterNode n = gossip.nodeManager.clusterLookupNode(argv[2]);

            if (n == null) {
                replyError(t, "Unknown node " + argv[2]);
                return;
            }

            if (n.equals(server.myself)) {
                replyError(t, "Can't replicate myself");
                return;
            }

            if (nodeIsSlave(n)) {
                replyError(t, "I can only replicate a master, not a slave.");
                return;
            }

            if (nodeIsMaster(server.myself) && (server.myself.numslots != 0)) {
                replyError(t, "To set a master the node must be empty and without assigned slots.");
                return;
            }

            gossip.clusterSetMaster(n);
            gossip.clusterDoBeforeSleep(CLUSTER_TODO_UPDATE_STATE | CLUSTER_TODO_SAVE_CONFIG);
            reply(t, "OK");
        } else if (argv[1].equalsIgnoreCase("slaves") && argv.length == 3) {
            /* CLUSTER SLAVES <NODE ID> */
            ClusterNode n = gossip.nodeManager.clusterLookupNode(argv[2]);

            if (n == null) {
                replyError(t, "Unknown node " + argv[2]);
                return;
            }

            if (nodeIsSlave(n)) {
                replyError(t, "The specified node is not a master");
                return;
            }

            StringBuilder ci = new StringBuilder();
            for (int j = 0; j < n.numslaves; j++) {
                ci.append(gossip.configManager.clusterGenNodeDescription(n.slaves.get(j)));
            }
            reply(t, ci.toString());
        } else if (argv[1].equalsIgnoreCase("count-failure-reports") && argv.length == 3) {
            /* CLUSTER COUNT-FAILURE-REPORTS <NODE ID> */
            ClusterNode n = gossip.nodeManager.clusterLookupNode(argv[2]);

            if (n == null) {
                replyError(t, "Unknown node " + argv[2]);
                return;
            } else {
                reply(t, String.valueOf(gossip.nodeManager.clusterNodeFailureReportsCount(n)));
            }
        } else if (argv[1].equalsIgnoreCase("set-config-epoch") && argv.length == 3) {
            long epoch = parseLong(argv[2]);

            if (epoch < 0) {
                replyError(t, "Invalid config epoch specified: " + epoch);
            } else if (server.cluster.nodes.size() > 1) {
                replyError(t, "The user can assign a config epoch only when the node does not know any other node.");
            } else if (server.myself.configEpoch != 0) {
                replyError(t, "Node config epoch is already non-zero");
            } else {
                server.myself.configEpoch = epoch;
                logger.warn("configEpoch set to " + server.myself.configEpoch + " via CLUSTER SET-CONFIG-EPOCH");
                if (server.cluster.currentEpoch < epoch)
                    server.cluster.currentEpoch = epoch;
                gossip.clusterDoBeforeSleep(CLUSTER_TODO_UPDATE_STATE | CLUSTER_TODO_SAVE_CONFIG);
                reply(t, "OK");
            }
        } else if (argv[1].equalsIgnoreCase("reset") && (argv.length == 2 || argv.length == 3)) {
            //unsupport
        } else {
            replyError(t, "Wrong CLUSTER subcommand or number of arguments");
        }
    }

    public boolean clusterBumpConfigEpochWithoutConsensus() {
        long maxEpoch = gossip.clusterGetMaxEpoch();

        if (server.myself.configEpoch == 0 || server.myself.configEpoch != maxEpoch) {
            server.cluster.currentEpoch++;
            server.myself.configEpoch = server.cluster.currentEpoch;
            gossip.clusterDoBeforeSleep(CLUSTER_TODO_SAVE_CONFIG);
            logger.warn("New configEpoch set to " + server.myself.configEpoch);
            return true;
        }
        return false;
    }

    private void replyString(Transport t, String s) {

    }

    private void replyError(Transport t, String s) {

    }

    private void reply(Transport t, String s) {

    }
}

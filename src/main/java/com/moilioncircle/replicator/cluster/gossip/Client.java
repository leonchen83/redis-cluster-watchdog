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
    private ClusterNode myself;

    public Client(ThinGossip gossip) {
        this.gossip = gossip;
        this.server = gossip.server;
        this.myself = gossip.myself;
    }

    public void clusterCommand(Transport t, String[] argv) {
        if (!server.clusterEnabled) {
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
            reply(t, myself.name);
        } else if (argv[1].equalsIgnoreCase("flushslots") && argv.length == 2) {
            /* CLUSTER FLUSHSLOTS */
            gossip.slotManger.clusterDelNodeSlots(myself);
            gossip.clusterDoBeforeSleep(CLUSTER_TODO_UPDATE_STATE | CLUSTER_TODO_SAVE_CONFIG);
            reply(t, "OK");
        } else if ((argv[1].equalsIgnoreCase("addslots") || argv[1].equalsIgnoreCase("delslots")) && argv.length >= 3) {
            /* CLUSTER ADDSLOTS <slot> [slot] ... */
            /* CLUSTER DELSLOTS <slot> [slot] ... */

            byte[] slots = new byte[CLUSTER_SLOTS];
            boolean del = argv[1].equalsIgnoreCase("delslots");

            for (int j = 2; j < argv.length; j++) {
                int slot = parseInt(argv[j]);
                if (slot < 0 || slot >= CLUSTER_SLOTS) {
                    replyError(t, "Invalid or out of range slot");
                    return;
                }
                if (del && server.cluster.slots[slot] == null) {
                    replyError(t, "Slot " + slot + " is already unassigned");
                    return;
                } else if (!del && server.cluster.slots[slot] != null) {
                    replyError(t, "Slot " + slot + " is already busy");
                    return;
                }
                if (slots[slot]++ == 1) {
                    replyError(t, "Slot " + slot + " specified multiple times");
                    return;
                }
            }
            for (int j = 0; j < CLUSTER_SLOTS; j++) {
                if (slots[j] != 0) {
                    if (server.cluster.importingSlotsFrom[j] != null)
                        server.cluster.importingSlotsFrom[j] = null;
                    boolean retval = del ? gossip.slotManger.clusterDelSlot(j) : gossip.slotManger.clusterAddSlot(myself, j);
                }
            }
            gossip.clusterDoBeforeSleep(CLUSTER_TODO_UPDATE_STATE | CLUSTER_TODO_SAVE_CONFIG);
            reply(t, "OK");
        } else if (argv[1].equalsIgnoreCase("setslot") && argv.length >= 4) {
            /* SETSLOT 10 MIGRATING <node ID> */
            /* SETSLOT 10 IMPORTING <node ID> */
            /* SETSLOT 10 STABLE */
            /* SETSLOT 10 NODE <node ID> */
            if (nodeIsSlave(myself)) {
                replyError(t, "Please use SETSLOT only with masters.");
                return;
            }

            int slot = parseInt(argv[2]);
            if (slot < 0 || slot >= CLUSTER_SLOTS) {
                replyError(t, "Invalid or out of range slot");
                return;
            }

            if (argv[3].equalsIgnoreCase("migrating") && argv.length == 5) {
                if (!server.cluster.slots[slot].equals(myself)) {
                    replyError(t, "I'm not the owner of hash slot " + slot);
                    return;
                }
                ClusterNode n;
                if ((n = gossip.nodeManager.clusterLookupNode(argv[4])) == null) {
                    replyError(t, "I don't know about node " + argv[4]);
                    return;
                }
                server.cluster.migratingSlotsTo[slot] = n;
            } else if (argv[3].equalsIgnoreCase("importing") && argv.length == 5) {
                if (server.cluster.slots[slot].equals(myself)) {
                    replyError(t, "I'm already the owner of hash slot " + slot);
                    return;
                }
                ClusterNode n;
                if ((n = gossip.nodeManager.clusterLookupNode(argv[4])) == null) {
                    replyError(t, "I don't know about node " + argv[3]);
                    return;
                }
                server.cluster.importingSlotsFrom[slot] = n;
            } else if (argv[3].equalsIgnoreCase("stable") && argv.length == 4) {
                /* CLUSTER SETSLOT <SLOT> STABLE */
                server.cluster.importingSlotsFrom[slot] = null;
                server.cluster.migratingSlotsTo[slot] = null;
            } else if (argv[3].equalsIgnoreCase("node") && argv.length == 5) {
                /* CLUSTER SETSLOT <SLOT> NODE <NODE ID> */
                ClusterNode n = gossip.nodeManager.clusterLookupNode(argv[4]);

                if (n == null) {
                    replyError(t, "Unknown node " + argv[4]);
                    return;
                }
                if (server.cluster.slots[slot].equals(myself) && !n.equals(myself)) {
                    if (gossip.slotManger.countKeysInSlot(slot) != 0) {
                        replyError(t, "Can't assign hashslot " + slot + " to a different node while I still hold keys for this hash slot.");
                        return;
                    }
                }
                if (gossip.slotManger.countKeysInSlot(slot) == 0 && server.cluster.migratingSlotsTo[slot] != null)
                    server.cluster.migratingSlotsTo[slot] = null;

                if (n.equals(myself) && server.cluster.importingSlotsFrom[slot] != null) {
                    if (clusterBumpConfigEpochWithoutConsensus()) {
                        logger.warn("configEpoch updated after importing slot " + slot);
                    }
                    server.cluster.importingSlotsFrom[slot] = null;
                }
                gossip.slotManger.clusterDelSlot(slot);
                gossip.slotManger.clusterAddSlot(n, slot);
            } else {
                replyError(t, "Invalid CLUSTER SETSLOT action or number of arguments");
                return;
            }
            gossip.clusterDoBeforeSleep(CLUSTER_TODO_SAVE_CONFIG | CLUSTER_TODO_UPDATE_STATE);
            reply(t, "OK");
        } else if (argv[1].equalsIgnoreCase("bumpepoch") && argv.length == 2) {
            /* CLUSTER BUMPEPOCH */
            boolean retval = clusterBumpConfigEpochWithoutConsensus();
            String reply = new StringBuilder("+").append(retval ? "BUMPED" : "STILL").append(" ").append(myself.configEpoch).append("\r\n").toString();
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
                } else if (nodeTimedOut(n)) {
                    slotsPfail++;
                } else {
                    slotsOk++;
                }
            }

            long myepoch = (nodeIsSlave(myself) && myself.slaveof != null) ? myself.slaveof.configEpoch : myself.configEpoch;

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
            /* CLUSTER COUNTKEYSINSLOT <slot> */
            int slot = parseInt(argv[2]);
            if (slot < 0 || slot >= CLUSTER_SLOTS) {
                replyError(t, "Invalid slot");
                return;
            }
            reply(t, String.valueOf(gossip.slotManger.countKeysInSlot(slot)));
        } else if (argv[1].equalsIgnoreCase("forget") && argv.length == 3) {
            /* CLUSTER FORGET <NODE ID> */
            ClusterNode n = gossip.nodeManager.clusterLookupNode(argv[2]);

            if (n == null) {
                replyError(t, "Unknown node " + argv[2]);
                return;
            } else if (n.equals(myself)) {
                replyError(t, "I tried hard but I can't forget myself...");
                return;
            } else if (nodeIsSlave(myself) && myself.slaveof.equals(n)) {
                replyError(t, "Can't forget my master!");
                return;
            }
            gossip.clusterBlacklistAddNode(n);
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

            if (n.equals(myself)) {
                replyError(t, "Can't replicate myself");
                return;
            }

            if (nodeIsSlave(n)) {
                replyError(t, "I can only replicate a master, not a slave.");
                return;
            }

            if (nodeIsMaster(myself) && (myself.numslots != 0 /*|| dictSize(server.db[0].dict) != 0 */)) {
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

            for (int j = 0; j < n.numslaves; j++) {
                String ni = gossip.configManager.clusterGenNodeDescription(n.slaves.get(j));
            }
            //TODO
        } else if (argv[1].equalsIgnoreCase("count-failure-reports") && argv.length == 3) {
            /* CLUSTER COUNT-FAILURE-REPORTS <NODE ID> */
            ClusterNode n = gossip.nodeManager.clusterLookupNode(argv[2]);

            if (n == null) {
                replyError(t, "Unknown node " + argv[2]);
                return;
            } else {
                reply(t, String.valueOf(gossip.nodeManager.clusterNodeFailureReportsCount(n)));
            }
        } else if (argv[1].equalsIgnoreCase("failover") && (argv.length == 2 || argv.length == 3)) {
            /* CLUSTER FAILOVER [FORCE|TAKEOVER] */
            boolean force = false, takeover = false;

            if (argv.length == 3) {
                if (argv[2].equalsIgnoreCase("force")) {
                    force = true;
                } else if (argv[2].equalsIgnoreCase("takeover")) {
                    takeover = true;
                    force = true;
                } else {
                    reply(t, "syntax error");
                    return;
                }
            }

            /* Check preconditions. */
            if (nodeIsMaster(myself)) {
                replyError(t, "You should send CLUSTER FAILOVER to a slave");
                return;
            } else if (myself.slaveof == null) {
                replyError(t, "I'm a slave but my master is unknown to me");
                return;
            } else if (!force && (nodeFailed(myself.slaveof) || myself.slaveof.link == null)) {
                replyError(t, "Master is down or failed, please use CLUSTER FAILOVER FORCE");
                return;
            }
            gossip.resetManualFailover();
            server.cluster.mfEnd = System.currentTimeMillis() + CLUSTER_MF_TIMEOUT;

            if (takeover) {
                logger.warn("Taking over the master (user request).");
                clusterBumpConfigEpochWithoutConsensus();
                gossip.clusterFailoverReplaceYourMaster();
            } else if (force) {
                logger.warn("Forced failover user request accepted.");
                server.cluster.mfCanStart = true;
            } else {
                logger.warn("Manual failover user request accepted.");
                gossip.msgManager.clusterSendMFStart(myself.slaveof);
            }
            reply(t, "OK");
        } else if (argv[1].equalsIgnoreCase("set-config-epoch") && argv.length == 3) {
            long epoch = parseLong(argv[2]);

            if (epoch < 0) {
                replyError(t, "Invalid config epoch specified: " + epoch);
            } else if (server.cluster.nodes.size() > 1) {
                replyError(t, "The user can assign a config epoch only when the node does not know any other node.");
            } else if (myself.configEpoch != 0) {
                replyError(t, "Node config epoch is already non-zero");
            } else {
                myself.configEpoch = epoch;
                logger.warn("configEpoch set to " + myself.configEpoch + " via CLUSTER SET-CONFIG-EPOCH");
                if (server.cluster.currentEpoch < epoch)
                    server.cluster.currentEpoch = epoch;
                gossip.clusterDoBeforeSleep(CLUSTER_TODO_UPDATE_STATE | CLUSTER_TODO_SAVE_CONFIG);
                reply(t, "OK");
            }
        } else if (argv[1].equalsIgnoreCase("reset") && (argv.length == 2 || argv.length == 3)) {
            boolean hard = false;
            if (argv.length == 3) {
                if (argv[2].equalsIgnoreCase("hard")) {
                    hard = true;
                } else if (argv[2].equalsIgnoreCase("soft")) {
                    hard = false;
                } else {
                    reply(t, "syntax error");
                    return;
                }
            }

            if (nodeIsMaster(myself) /* && dictSize(c -> db -> dict) != 0 */) {
                replyError(t, "CLUSTER RESET can't be called with master nodes containing keys");
                return;
            }
            gossip.clusterReset(hard);
            reply(t, "OK");
        } else {
            replyError(t, "Wrong CLUSTER subcommand or number of arguments");
        }
    }

    public boolean clusterBumpConfigEpochWithoutConsensus() {
        long maxEpoch = gossip.clusterGetMaxEpoch();

        if (myself.configEpoch == 0 || myself.configEpoch != maxEpoch) {
            server.cluster.currentEpoch++;
            myself.configEpoch = server.cluster.currentEpoch;
            gossip.clusterDoBeforeSleep(CLUSTER_TODO_SAVE_CONFIG);
            logger.warn("New configEpoch set to " + myself.configEpoch);
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

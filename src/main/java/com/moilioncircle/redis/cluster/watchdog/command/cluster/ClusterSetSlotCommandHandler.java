/*
 * Copyright 2016-2017 Leon Chen
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

package com.moilioncircle.redis.cluster.watchdog.command.cluster;

import com.moilioncircle.redis.cluster.watchdog.command.AbstractCommandHandler;
import com.moilioncircle.redis.cluster.watchdog.manager.ClusterManagers;
import com.moilioncircle.redis.cluster.watchdog.state.ClusterNode;
import com.moilioncircle.redis.cluster.watchdog.util.net.transport.Transport;

import static com.moilioncircle.redis.cluster.watchdog.state.NodeStates.nodeIsSlave;
import static java.lang.Integer.parseInt;

/**
 * @author Leon Chen
 * @since 1.0.0
 */
public class ClusterSetSlotCommandHandler extends AbstractCommandHandler {

    public ClusterSetSlotCommandHandler(ClusterManagers managers) {
        super(managers);
    }

    @Override
    public void handle(Transport<Object> t, String[] message, byte[][] rawMessage) {

        if (!managers.configuration.isAsMaster()) {
            replyError(t, "Unsupported COMMAND");
            return;
        }

        if (message.length < 4) {
            replyError(t, "Wrong CLUSTER subcommand or number of arguments");
            return;
        }

        if (nodeIsSlave(server.myself)) {
            replyError(t, "Please use SETSLOT only with masters.");
            return;
        }

        int slot;
        try {
            slot = parseInt(message[2]);
        } catch (Exception e) {
            replyError(t, "Invalid slot:" + message[2]);
            return;
        }

        if (slot < 0 || slot > 16384) {
            replyError(t, "Invalid slot:" + slot);
            return;
        }

        if (message[3] == null) {
            replyError(t, "Wrong CLUSTER subcommand or number of arguments");
        }
        if (message[3].equalsIgnoreCase("migrating") && message.length == 5) {
            if (server.cluster.slots[slot] == null || !server.cluster.slots[slot].equals(server.myself)) {
                replyError(t, "I'm not the owner of hash slot " + slot);
                return;
            }
            ClusterNode n = managers.nodes.clusterLookupNode(message[4]);
            if (n == null) {
                replyError(t, "I don't know fail node " + message[4]);
                return;
            }
            server.cluster.migratingSlotsTo[slot] = n;
        } else if (message[3].equalsIgnoreCase("importing") && message.length == 5) {
            if (server.cluster.slots[slot] != null && server.cluster.slots[slot].equals(server.myself)) {
                replyError(t, "I'm already the owner of hash slot " + slot);
                return;
            }
            ClusterNode n = managers.nodes.clusterLookupNode(message[4]);
            if (n == null) {
                replyError(t, "I don't know fail node " + message[4]);
                return;
            }
            server.cluster.importingSlotsFrom[slot] = n;
        } else if (message[3].equalsIgnoreCase("stable") && message.length == 4) {
            /* CLUSTER SETSLOT <SLOT> STABLE */
            server.cluster.importingSlotsFrom[slot] = null;
            server.cluster.migratingSlotsTo[slot] = null;
        } else if (message[3].equalsIgnoreCase("node") && message.length == 5) {
            /* CLUSTER SETSLOT <SLOT> NODE <NODE ID> */
            ClusterNode n = managers.nodes.clusterLookupNode(message[4]);

            if (n == null) {
                replyError(t, "Unknown node " + message[4]);
                return;
            }

            if (server.cluster.migratingSlotsTo[slot] != null)
                server.cluster.migratingSlotsTo[slot] = null;

            if (n.equals(server.myself) && server.cluster.importingSlotsFrom[slot] != null) {
                if (managers.states.clusterBumpConfigEpochWithoutConsensus()) {
                    logger.warn("configEpoch updated after importing slot " + slot);
                }
                server.cluster.importingSlotsFrom[slot] = null;
            }
            managers.slots.clusterDelSlot(slot);
            managers.slots.clusterAddSlot(n, slot);
        } else {
            replyError(t, "Invalid CLUSTER SETSLOT action or number of arguments");
            return;
        }
        managers.states.clusterUpdateState();
        reply(t, "OK");
    }
}
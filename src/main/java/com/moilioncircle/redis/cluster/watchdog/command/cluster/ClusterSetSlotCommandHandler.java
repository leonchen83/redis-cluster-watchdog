/*
 * Copyright 2016-2018 Leon Chen
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
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.Objects;

import static com.moilioncircle.redis.cluster.watchdog.ClusterConstants.CLUSTER_SLOTS;
import static com.moilioncircle.redis.cluster.watchdog.state.NodeStates.nodeIsSlave;
import static java.lang.Integer.parseInt;

/**
 * @author Leon Chen
 * @since 1.0.0
 */
public class ClusterSetSlotCommandHandler extends AbstractCommandHandler {

    private static final Log logger = LogFactory.getLog(ClusterSetSlotCommandHandler.class);

    public ClusterSetSlotCommandHandler(ClusterManagers managers) {
        super(managers);
    }

    @Override
    public void handle(Transport<byte[][]> t, String[] message, byte[][] rawMessage) {
        if (message.length < 4) {
            replyError(t, "ERR Wrong CLUSTER subcommand or number of arguments"); return;
        }

        if (nodeIsSlave(server.myself)) {
            replyError(t, "ERR Please use SETSLOT only with masters."); return;
        }

        int slot;
        try { slot = parseInt(message[2]); }
        catch (Exception e) { replyError(t, "ERR Invalid slot:" + message[2]); return; }
        if (slot < 0 || slot > CLUSTER_SLOTS) { replyError(t, "ERR Invalid slot:" + slot); return; }

        if (message[3] == null) {
            replyError(t, "ERR Wrong CLUSTER subcommand or number of arguments"); return;
        }

        if (message[3].equalsIgnoreCase("migrating") && message.length == 5) {
            if (!Objects.equals(server.cluster.slots[slot], server.myself)) {
                replyError(t, "ERR I'm not the owner of hash slot " + slot); return;
            }
            ClusterNode n = managers.nodes.clusterLookupNode(message[4]);
            if (n == null) { replyError(t, "ERR I don't know fail node " + message[4]); return; }
            server.cluster.migrating[slot] = n;
        } else if (message[3].equalsIgnoreCase("importing") && message.length == 5) {
            if (Objects.equals(server.cluster.slots[slot], server.myself)) {
                replyError(t, "ERR I'm already the owner of hash slot " + slot); return;
            }
            ClusterNode n = managers.nodes.clusterLookupNode(message[4]);
            if (n == null) { replyError(t, "ERR I don't know fail node " + message[4]); return; }
            server.cluster.importing[slot] = n;
        } else if (message[3].equalsIgnoreCase("stable") && message.length == 4) {
            server.cluster.importing[slot] = null; server.cluster.migrating[slot] = null;
        } else if (message[3].equalsIgnoreCase("node") && message.length == 5) {
            /* CLUSTER SETSLOT <SLOT> NODE <NODE ID> */
            ClusterNode n = managers.nodes.clusterLookupNode(message[4]);
            if (n == null) { replyError(t, "ERR Unknown node " + message[4]); return; }
            if (Objects.equals(server.cluster.slots[slot], server.myself)
                    && !Objects.equals(n, server.myself)
                    && managers.slots.countKeysInSlot(slot) != 0) {
                replyError(t, "ERR Can't assign hashslot " + slot + " to a different node while I still hold keys for this hash slot.");
                return;
            }
            if (managers.slots.countKeysInSlot(slot) == 0 && server.cluster.migrating[slot] != null)
                server.cluster.migrating[slot] = null;
            if (Objects.equals(n, server.myself) && server.cluster.importing[slot] != null) {
                if (managers.states.clusterBumpConfigEpochWithoutConsensus()) {
                    logger.info("configEpoch updated after importing slot " + slot);
                }
                server.cluster.importing[slot] = null;
            }
            managers.slots.clusterDelSlot(slot); managers.slots.clusterAddSlot(n, slot);
        } else {
            replyError(t, "ERR Invalid CLUSTER SETSLOT action or number of arguments"); return;
        }
        managers.states.clusterUpdateState(); reply(t, "OK");
    }
}
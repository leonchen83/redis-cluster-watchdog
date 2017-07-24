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
import com.moilioncircle.redis.cluster.watchdog.util.net.transport.Transport;

import static com.moilioncircle.redis.cluster.watchdog.ClusterConstants.CLUSTER_SLOTS;
import static java.lang.Integer.parseInt;

/**
 * @author Leon Chen
 * @since 1.0.0
 */
public class ClusterDelSlotsCommandHandler extends AbstractCommandHandler {

    public ClusterDelSlotsCommandHandler(ClusterManagers managers) {
        super(managers);
    }

    @Override
    public void handle(Transport<Object> t, String[] message, byte[][] rawMessage) {

        if (!managers.configuration.isAsMaster()) {
            replyError(t, "Unsupported COMMAND");
            return;
        }

        if (message.length < 3) {
            replyError(t, "Wrong CLUSTER subcommand or number of arguments");
            return;
        }

        byte[] slots = new byte[CLUSTER_SLOTS];
        for (int i = 2; i < message.length; i++) {
            try {
                int slot = parseInt(message[i]);
                if (slot < 0 || slot > 16384) {
                    replyError(t, "Invalid slot:" + slot);
                    return;
                }

                if (server.cluster.slots[slot] == null) {
                    replyError(t, "Slot " + slot + " is already unassigned");
                    return;
                }

                if (slots[slot]++ == 1) {
                    replyError(t, "Slot " + slot + " specified multiple times");
                    return;
                }
            } catch (Exception e) {
                replyError(t, "Invalid slot:" + message[i]);
                return;
            }
        }
        for (int i = 0; i < CLUSTER_SLOTS; i++) {
            if (slots[i] == 0) continue;
            if (server.cluster.importingSlotsFrom[i] != null)
                server.cluster.importingSlotsFrom[i] = null;
            managers.slots.clusterDelSlot(i);
        }
        managers.states.clusterUpdateState();
        reply(t, "OK");
    }
}
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
        if (message.length < 3) {
            t.write(("-ERR Wrong CLUSTER subcommand or number of arguments\r\n").getBytes(), true);
            return;
        }
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
        t.write(("-ERR Unsupported operation [cluster " + message[1] + "]\r\n").getBytes(), true);
    }
}
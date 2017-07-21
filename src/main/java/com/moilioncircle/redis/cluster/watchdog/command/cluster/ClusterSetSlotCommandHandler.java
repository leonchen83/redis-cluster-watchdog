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
public class ClusterSetSlotCommandHandler extends AbstractCommandHandler {

    public ClusterSetSlotCommandHandler(ClusterManagers managers) {
        super(managers);
    }

    @Override
    public void handle(Transport<Object> t, String[] message, byte[][] rawMessage) {
        if (message.length < 4) {
            replyError(t, "Wrong CLUSTER subcommand or number of arguments");
            return;
        }
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
//                    t.write(("-ERR I don't know fail node " + argv[4] + "\r\n").getBytes(), true);
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
//                    t.write(("-ERR I don't know fail node " + argv[4] + "\r\n").getBytes(), true);
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
        replyError(t, "Unsupported operation [cluster " + message[1] + "]");
    }
}
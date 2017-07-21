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

import static com.moilioncircle.redis.cluster.watchdog.ClusterConstants.CLUSTER_SLOTS;
import static com.moilioncircle.redis.cluster.watchdog.manager.ClusterSlotManger.bitmapTestBit;
import static com.moilioncircle.redis.cluster.watchdog.state.States.nodeFailed;
import static com.moilioncircle.redis.cluster.watchdog.state.States.nodeIsMaster;

/**
 * @author Leon Chen
 * @since 1.0.0
 */
public class ClusterSlotsCommandHandler extends AbstractCommandHandler {

    public ClusterSlotsCommandHandler(ClusterManagers managers) {
        super(managers);
    }

    @Override
    public void handle(Transport<Object> t, String[] message, byte[][] rawMessage) {
        if (message.length != 2) {
            replyError(t, "Wrong CLUSTER subcommand or number of arguments");
            return;
        }

        t.write(clusterReplyMultiBulkSlots().getBytes(), true);
    }

    protected String clusterReplyMultiBulkSlots() {
        int masters = 0;
        StringBuilder r = new StringBuilder();
        for (ClusterNode node : server.cluster.nodes.values()) {
            int start = -1;
            if (!nodeIsMaster(node) || node.assignedSlots == 0) continue;

            for (int i = 0; i < CLUSTER_SLOTS; i++) {
                boolean bit;
                if ((bit = bitmapTestBit(node.slots, i)) && start == -1) {
                    start = i;
                }
                if (start != -1 && (!bit || i == CLUSTER_SLOTS - 1)) {
                    StringBuilder builder = new StringBuilder();
                    int elements = 3;
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
                    for (ClusterNode slave : node.slaves) {
                        if (nodeFailed(slave)) continue;
                        builder.append("*3\r\n");
                        builder.append("$").append(slave.ip.length()).append("\r\n").append(slave.ip).append("\r\n");
                        builder.append(":").append(slave.port).append("\r\n");
                        builder.append("$").append(slave.name.length()).append("\r\n").append(slave.name).append("\r\n");
                        elements++;
                    }
                    builder.insert(0, "*" + elements + "\r\n");
                    r.append(builder.toString());
                    masters++;
                }
            }
        }
        r.insert(0, "*" + masters + "\r\n");
        return r.toString();
    }
}

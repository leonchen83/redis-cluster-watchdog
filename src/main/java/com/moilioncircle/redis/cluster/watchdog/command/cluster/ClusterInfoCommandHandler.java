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

import static com.moilioncircle.redis.cluster.watchdog.ClusterConstants.CLUSTERMSG_TYPE_COUNT;
import static com.moilioncircle.redis.cluster.watchdog.ClusterConstants.CLUSTER_SLOTS;
import static com.moilioncircle.redis.cluster.watchdog.state.States.*;

/**
 * @author Leon Chen
 * @since 1.0.0
 */
public class ClusterInfoCommandHandler extends AbstractCommandHandler {

    public ClusterInfoCommandHandler(ClusterManagers managers) {
        super(managers);
    }

    @Override
    public void handle(Transport<Object> t, String[] message, byte[][] rawMessage) {
        if (message.length != 2) {
            t.write(("-ERR Wrong CLUSTER subcommand or number of arguments\r\n").getBytes(), true);
            return;
        }

        String[] stats = {"ok", "fail", "needhelp"};
        int assigned = 0, normal = 0, fail = 0, pFail = 0;

        for (int j = 0; j < CLUSTER_SLOTS; j++) {
            ClusterNode node = server.cluster.slots[j];

            if (node == null) continue;
            assigned++;
            if (nodeFailed(node)) {
                fail++;
            } else if (nodePFailed(node)) {
                pFail++;
            } else {
                normal++;
            }
        }

        long epoch = (nodeIsSlave(server.myself) && server.myself.master != null) ? server.myself.master.configEpoch : server.myself.configEpoch;

        StringBuilder info = new StringBuilder("cluster_state:").append(stats[server.cluster.state]).append("\r\n")
                .append("cluster_slots_assigned:").append(assigned).append("\r\n")
                .append("cluster_slots_ok:").append(normal).append("\r\n")
                .append("cluster_slots_pfail:").append(pFail).append("\r\n")
                .append("cluster_slots_fail:").append(fail).append("\r\n")
                .append("cluster_known_nodes:").append(server.cluster.nodes.size()).append("\r\n")
                .append("cluster_size:").append(server.cluster.size).append("\r\n")
                .append("cluster_current_epoch:").append(server.cluster.currentEpoch).append("\r\n")
                .append("cluster_my_epoch:").append(epoch).append("\r\n");


        long sent = 0;
        long received = 0;

        for (int i = 0; i < CLUSTERMSG_TYPE_COUNT; i++) {
            if (server.cluster.messagesSent[i] == 0) continue;
            sent += server.cluster.messagesSent[i];
            info.append("cluster_stats_messages_").append(managers.configs.clusterGetMessageTypeString(i)).append("_sent:").append(server.cluster.messagesSent[i]).append("\r\n");
        }

        info.append("cluster_stats_messages_sent:").append(sent).append("\r\n");

        for (int i = 0; i < CLUSTERMSG_TYPE_COUNT; i++) {
            if (server.cluster.messagesReceived[i] == 0) continue;
            received += server.cluster.messagesReceived[i];
            info.append("cluster_stats_messages_").append(managers.configs.clusterGetMessageTypeString(i)).append("_received:").append(server.cluster.messagesReceived[i]).append("\r\n");
        }

        info.append("cluster_stats_messages_received:").append(received).append("\r\n");
        t.write(("$" + info.length() + "\r\n" + info.toString() + "\r\n").getBytes(), true);
    }
}

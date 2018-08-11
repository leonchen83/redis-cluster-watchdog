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

import static com.moilioncircle.redis.cluster.watchdog.ClusterConstants.CLUSTERMSG_TYPE_COUNT;
import static com.moilioncircle.redis.cluster.watchdog.ClusterConstants.CLUSTER_SLOTS;
import static com.moilioncircle.redis.cluster.watchdog.manager.ClusterConfigManager.clusterGetMessageTypeString;
import static com.moilioncircle.redis.cluster.watchdog.state.NodeStates.nodeFailed;
import static com.moilioncircle.redis.cluster.watchdog.state.NodeStates.nodeIsSlave;
import static com.moilioncircle.redis.cluster.watchdog.state.NodeStates.nodePFailed;

/**
 * @author Leon Chen
 * @since 1.0.0
 */
public class ClusterInfoCommandHandler extends AbstractCommandHandler {

    public ClusterInfoCommandHandler(ClusterManagers managers) {
        super(managers);
    }

    @Override
    public void handle(Transport<byte[][]> t, String[] message, byte[][] rawMessage) {
        if (message.length != 2) {
            replyError(t, "ERR Wrong CLUSTER subcommand or number of arguments");
            return;
        }

        int assigned = 0, normal = 0, fail = 0, pFail = 0;
        for (int j = 0; j < CLUSTER_SLOTS; j++) {
            ClusterNode node = server.cluster.slots[j];
            if (node == null) continue;
            assigned++;
            if (nodeFailed(node)) fail++;
            else if (nodePFailed(node)) pFail++;
            else normal++;
        }

        long epoch;
        if (nodeIsSlave(server.myself) && server.myself.master != null) {
            epoch = server.myself.master.configEpoch;
        } else {
            epoch = server.myself.configEpoch;
        }

        int size = server.cluster.size;
        int nodes = server.cluster.nodes.size();
        long currentEpoch = server.cluster.currentEpoch;
        String state = server.cluster.state.getDisplay();

        StringBuilder info = new StringBuilder();
        info.append("cluster_state:").append(state).append("\r\n");
        info.append("cluster_slots_assigned:").append(assigned).append("\r\n");
        info.append("cluster_slots_ok:").append(normal).append("\r\n");
        info.append("cluster_slots_pfail:").append(pFail).append("\r\n");
        info.append("cluster_slots_fail:").append(fail).append("\r\n");
        info.append("cluster_known_nodes:").append(nodes).append("\r\n");
        info.append("cluster_size:").append(size).append("\r\n");
        info.append("cluster_current_epoch:").append(currentEpoch).append("\r\n");
        info.append("cluster_my_epoch:").append(epoch).append("\r\n");

        long sent = 0L, received = 0L;
        for (int i = 0; i < CLUSTERMSG_TYPE_COUNT; i++) {
            if (server.cluster.messagesSent[i] == 0) continue;
            sent += server.cluster.messagesSent[i];
            info.append("cluster_stats_messages_");
            info.append(clusterGetMessageTypeString(i)).append("_sent:");
            info.append(server.cluster.messagesSent[i]).append("\r\n");
        }
        info.append("cluster_stats_messages_sent:").append(sent).append("\r\n");
        for (int i = 0; i < CLUSTERMSG_TYPE_COUNT; i++) {
            if (server.cluster.messagesReceived[i] == 0) continue;
            received += server.cluster.messagesReceived[i];
            info.append("cluster_stats_messages_");
            info.append(clusterGetMessageTypeString(i)).append("_received:");
            info.append(server.cluster.messagesReceived[i]).append("\r\n");
        }
        info.append("cluster_stats_messages_received:").append(received).append("\r\n");

        replyBulk(t, info.toString());
    }
}

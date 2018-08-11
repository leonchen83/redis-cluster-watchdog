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

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static com.moilioncircle.redis.cluster.watchdog.ClusterConstants.CLUSTER_SLOTS;
import static com.moilioncircle.redis.cluster.watchdog.ClusterNodeInfo.valueOf;
import static com.moilioncircle.redis.cluster.watchdog.manager.ClusterNodeManager.getRandomHexChars;
import static com.moilioncircle.redis.cluster.watchdog.state.NodeStates.nodeIsSlave;

/**
 * @author Leon Chen
 * @since 1.0.0
 */
public class ClusterResetCommandHandler extends AbstractCommandHandler {

    private static final Log logger = LogFactory.getLog(ClusterResetCommandHandler.class);

    public ClusterResetCommandHandler(ClusterManagers managers) {
        super(managers);
    }

    @Override
    public void handle(Transport<byte[][]> t, String[] message, byte[][] rawMessage) {
        if (message.length != 2 && message.length != 3) {
            replyError(t, "ERR Wrong CLUSTER subcommand or number of arguments");
            return;
        }

        boolean hard = false;
        if (message.length == 3) {
            if (message[2] != null && message[2].equalsIgnoreCase("hard")) hard = true;
            else if (message[2] != null && message[2].equalsIgnoreCase("soft")) hard = false;
            else {
                replyError(t, "ERR Wrong CLUSTER subcommand or number of arguments");
                return;
            }
        }
        clusterReset(hard);
        reply(t, "OK");
    }

    public void clusterReset(boolean hard) {
        if (nodeIsSlave(server.myself)) {
            managers.nodes.clusterSetNodeAsMaster(server.myself);
            managers.replications.replicationUnsetMaster();
        }
        managers.slots.clusterCloseAllSlots();
        for (int i = 0; i < CLUSTER_SLOTS; i++)
            managers.slots.clusterDelSlot(i);

        List<ClusterNode> nodes = new ArrayList<>(server.cluster.nodes.values());
        nodes.stream().filter(e -> !Objects.equals(e, server.myself)).forEach(e -> {
            managers.nodes.clusterDelNode(e);
            managers.notifyNodeDeleted(valueOf(e, server.myself));
        });

        if (!hard) return;
        server.myself.configEpoch = 0;
        server.cluster.currentEpoch = 0;
        server.cluster.lastVoteEpoch = 0;
        managers.notifyNodeDeleted(valueOf(server.myself, server.myself));
        managers.nodes.clusterRenameNode(server.myself, getRandomHexChars());
        logger.info("Node hard reset, now I'm " + server.myself.name);
    }
}

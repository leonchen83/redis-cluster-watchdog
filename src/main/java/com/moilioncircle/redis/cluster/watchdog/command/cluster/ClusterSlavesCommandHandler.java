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
import com.moilioncircle.redis.cluster.watchdog.config.NodeInfo;
import com.moilioncircle.redis.cluster.watchdog.manager.ClusterManagers;
import com.moilioncircle.redis.cluster.watchdog.state.ClusterNode;
import com.moilioncircle.redis.cluster.watchdog.util.net.transport.Transport;

import static com.moilioncircle.redis.cluster.watchdog.state.States.nodeIsSlave;

/**
 * @author Leon Chen
 * @since 1.0.0
 */
public class ClusterSlavesCommandHandler extends AbstractCommandHandler {

    public ClusterSlavesCommandHandler(ClusterManagers managers) {
        super(managers);
    }

    @Override
    public void handle(Transport<Object> t, String[] message, byte[][] rawMessage) {
        ClusterNode node = managers.nodes.clusterLookupNode(message[2]);

        if (node == null) {
            t.write(("-ERR Unknown node " + message[2] + "\r\n").getBytes(), true);
            return;
        }

        if (nodeIsSlave(node)) {
            t.write(("-ERR The specified node is not a master\r\n").getBytes(), true);
            return;
        }

        StringBuilder builder = new StringBuilder();
        builder.append("*").append(node.slaves.size()).append("\r\n");
        for (ClusterNode slave : node.slaves) {
            String str = managers.configs.clusterGenNodeDescription(NodeInfo.valueOf(slave, server.cluster.myself));
            builder.append("$").append(str.length()).append("\r\n").append(str).append("\r\n");
        }
        t.write(builder.toString().getBytes(), true);
    }
}

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

import static com.moilioncircle.redis.cluster.watchdog.ClusterConstants.CLUSTER_PORT_INCR;
import static java.lang.Integer.parseInt;

/**
 * @author Leon Chen
 * @since 1.0.0
 */
public class ClusterMeetCommandHandler extends AbstractCommandHandler {


    public ClusterMeetCommandHandler(ClusterManagers managers) {
        super(managers);
    }

    @Override
    public void handle(Transport<Object> t, String[] message, byte[][] rawMessage) {
        if (message.length != 4 && message.length != 5) {
            replyError(t, "Wrong CLUSTER subcommand or number of arguments");
            return;
        }

        int port, busPort;

        try {
            port = parseInt(message[3]);
        } catch (Exception e) {
            replyError(t, "Invalid port:" + message[3]);
            return;
        }

        if (message.length == 5) {
            try {
                busPort = parseInt(message[4]);
            } catch (Exception e) {
                replyError(t, "Invalid bus port:" + message[4]);
                return;
            }
        } else {
            busPort = port + CLUSTER_PORT_INCR;
        }

        if (port <= 0 || port > 65535) {
            replyError(t, "Invalid port:" + port);
            return;
        }

        if (busPort <= 0 || busPort > 65535) {
            replyError(t, "Invalid bus port:" + busPort);
            return;
        }

        if (message[2] == null || message[2].length() == 0) {
            replyError(t, "Invalid address:" + message[2]);
            return;
        }

        if (managers.nodes.clusterStartHandshake(message[2], port, busPort)) {
            reply(t, "OK");
        } else {
            replyError(t, "Invalid node address specified:" + message[2] + ":" + message[3]);
        }
    }
}

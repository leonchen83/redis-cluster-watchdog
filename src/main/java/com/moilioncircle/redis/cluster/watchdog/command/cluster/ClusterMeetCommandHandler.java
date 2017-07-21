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
            t.write(("-ERR Wrong CLUSTER subcommand or number of arguments\r\n").getBytes(), true);
            return;
        }

        int port, busPort;
        try {
            port = parseInt(message[3]);
        } catch (Exception e) {
            t.write(("-ERR invalid port:" + message[3]).getBytes(), true);
            return;
        }

        if (message.length == 5) {
            try {
                busPort = parseInt(message[4]);
            } catch (Exception e) {
                t.write(("-ERR invalid cport:" + message[4]).getBytes(), true);
                return;
            }
        } else {
            busPort = port + CLUSTER_PORT_INCR;
        }

        if (port <= 0 || port > 65535) {
            t.write(("-ERR invalid port:" + port).getBytes(), true);
            return;
        }

        if (busPort <= 0 || busPort > 65535) {
            t.write(("-ERR invalid cport:" + busPort).getBytes(), true);
            return;
        }

        if (message[2] == null || message[2].length() == 0) {
            t.write(("-ERR invalid ip address:" + message[2]).getBytes(), true);
            return;
        }

        if (managers.nodes.clusterStartHandshake(message[2], port, busPort)) {
            t.write("+OK\r\n".getBytes(), true);
        } else {
            t.write(("-ERR Invalid node address specified:" + message[2] + ":" + message[3] + "\r\n").getBytes(), true);
        }
    }
}

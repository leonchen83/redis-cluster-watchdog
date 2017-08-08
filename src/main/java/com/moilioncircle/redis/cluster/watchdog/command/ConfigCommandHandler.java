/*
 * Copyright 2016 leon chen
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

package com.moilioncircle.redis.cluster.watchdog.command;

import com.moilioncircle.redis.cluster.watchdog.manager.ClusterManagers;
import com.moilioncircle.redis.cluster.watchdog.util.net.transport.Transport;

import static java.lang.Long.parseLong;

/**
 * @author Leon Chen
 * @since 1.0.0
 */
public class ConfigCommandHandler extends AbstractCommandHandler {

    public ConfigCommandHandler(ClusterManagers managers) {
        super(managers);
    }

    @Override
    public void handle(Transport<byte[][]> t, String[] message, byte[][] rawMessage) {
        if (message.length == 4 && message[1] != null && message[1].equalsIgnoreCase("set")) {
            if (message[2] == null || !message[2].equalsIgnoreCase("cluster-node-timeout")) {
                replyError(t, "ERR wrong number of arguments for 'config' command"); return;
            }
            if (message[3] == null) {
                replyError(t, "ERR wrong number of arguments for 'config' command"); return;
            }
            try {
                long timeout = parseLong(message[3]);
                managers.configuration.setClusterNodeTimeout(timeout); reply(t, "OK");
            } catch (Exception e) { replyError(t, "ERR wrong number of arguments for 'config' command"); }
        } else if (message.length == 2 && message[1] != null && message[1].equalsIgnoreCase("rewrite")) {
            reply(t, "OK");
        } else {
            replyError(t, "ERR wrong number of arguments for 'config' command");
        }
    }
}

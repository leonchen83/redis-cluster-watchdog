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

import static java.lang.Integer.parseInt;
import static java.lang.Long.parseLong;

/**
 * @author Leon Chen
 * @since 1.0.0
 */
public class ClusterGetKeysInSlotCommandHandler extends AbstractCommandHandler {

    public ClusterGetKeysInSlotCommandHandler(ClusterManagers managers) {
        super(managers);
    }

    @Override
    public void handle(Transport<Object> t, String[] message, byte[][] rawMessage) {

        if (!managers.configuration.isMaster()) {
            replyError(t, "Unsupported COMMAND"); return;
        }

        if (message.length != 4) {
            replyError(t, "Wrong CLUSTER subcommand or number of arguments"); return;
        }

        int slot;
        try { slot = parseInt(message[2]); }
        catch (Exception e) { replyError(t, "Invalid slot:" + message[2]); return; }
        if (slot < 0 || slot > 16384) { replyError(t, "Invalid slot:" + slot); return; }

        long max;
        try { max = parseLong(message[3]); }
        catch (Exception e) { replyError(t, "Invalid number of keys:" + message[3]); return; }
        if (max < 0) { replyError(t, "Invalid number of keys:" + max); return; }

        t.write("*0\r\n".getBytes(), true);
    }
}

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

package com.moilioncircle.redis.cluster.watchdog.command;

import com.moilioncircle.redis.cluster.watchdog.manager.ClusterManagers;
import com.moilioncircle.redis.cluster.watchdog.util.net.transport.Transport;

import static java.lang.Long.parseLong;

/**
 * @author Leon Chen
 * @since 1.0.0
 */
public class RestoreCommandHandler extends AbstractCommandHandler {

    public RestoreCommandHandler(ClusterManagers managers) {
        super(managers);
    }

    @Override
    public void handle(Transport<byte[][]> t, String[] message, byte[][] rawMessage) {
        if (rawMessage.length != 4 && rawMessage.length != 5) {
            replyError(t, "ERR wrong number of arguments for 'restore' command"); return;
        }

        byte[] key = rawMessage[1];
        if (key == null) { replyError(t, "ERR Invalid key: null"); return; }

        long ttl;
        try { ttl = parseLong(message[2]); }
        catch (Exception e) { replyError(t, "ERR Invalid ttl: " + message[2]); return; }
        if (ttl < 0) { replyError(t, "ERR Invalid ttl: " + ttl); return; }

        byte[] serialized = rawMessage[3];
        if (serialized == null) { replyError(t, "ERR Invalid serialized-value: null"); return; }

        boolean replace;
        if (rawMessage.length == 5) {
            if (message[4] != null && message[4].equalsIgnoreCase("replace")) {
                replace = true;
            } else {
                replyError(t, "ERR wrong number of arguments for 'restore' command");
                return;
            }
        } else replace = false;

        long expire = ttl == 0 ? 0 : System.currentTimeMillis() + ttl;

        managers.engine.restore(key, serialized, expire, replace);
        reply(t, "OK");
    }
}

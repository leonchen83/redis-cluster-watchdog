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

package com.moilioncircle.redis.cluster.watchdog.command;

import com.moilioncircle.redis.cluster.watchdog.manager.ClusterManagers;
import com.moilioncircle.redis.cluster.watchdog.util.net.transport.Transport;

import static java.lang.Integer.parseInt;

/**
 * @author Leon Chen
 * @since 1.0.0
 */
public class SelectCommandHandler extends AbstractCommandHandler {
    
    public SelectCommandHandler(ClusterManagers managers) {
        super(managers);
    }
    
    @Override
    public void handle(Transport<byte[][]> t, String[] message, byte[][] rawMessage) {
        if (message.length != 2) {
            replyError(t, "ERR wrong number of arguments for 'select' command");
            return;
        }
        
        try {
            int db = parseInt(message[1]);
            if (db < 0 || db >= 16) {
                replyError(t, "ERR DB index is out of range");
                return;
            }
            if (db != 0) {
                replyError(t, "ERR SELECT is not allowed in cluster mode");
                return;
            }
            reply(t, "OK");
        } catch (Exception e) {
            replyError(t, "ERR Invalid db number:" + message[1]);
        }
    }
}

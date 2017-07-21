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

package com.moilioncircle.redis.cluster.watchdog.manager;

import com.moilioncircle.redis.cluster.watchdog.command.DefaultCommandHandler;
import com.moilioncircle.redis.cluster.watchdog.util.net.transport.Transport;

/**
 * @author Leon Chen
 * @since 1.0.0
 */
public class ClusterCommandHandlerManager {
    private DefaultCommandHandler handler;

    public ClusterCommandHandlerManager(ClusterManagers managers) {
        this.handler = new DefaultCommandHandler(managers);
    }

    public void handleCommand(Transport<Object> t, byte[][] rawMessage) {
        String[] message = new String[rawMessage.length];
        for (int i = 0; i < message.length; i++) {
            message[i] = new String(rawMessage[i]);
        }
        this.handler.handle(t, message, rawMessage);

    }
}

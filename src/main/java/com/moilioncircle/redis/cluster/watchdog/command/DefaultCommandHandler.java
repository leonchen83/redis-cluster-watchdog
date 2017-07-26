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

import com.moilioncircle.redis.cluster.watchdog.command.cluster.ClusterCommandHandler;
import com.moilioncircle.redis.cluster.watchdog.manager.ClusterManagers;
import com.moilioncircle.redis.cluster.watchdog.util.net.transport.Transport;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Leon Chen
 * @since 1.0.0
 */
public class DefaultCommandHandler extends AbstractCommandHandler {

    private Map<String, CommandHandler> handlers = new HashMap<>();

    public DefaultCommandHandler(ClusterManagers managers) {
        super(managers);
        register("ping", new PingCommandHandler(managers));
        register("info", new InfoCommandHandler(managers));
        register("dbsize", new DBSizeCommandHandler(managers));
        register("config", new ConfigCommandHandler(managers));
        register("cluster", new ClusterCommandHandler(managers));
        register("restore", new RestoreCommandHandler(managers));
        register("shutdown", new ShutdownCommandHandler(managers));
    }

    @Override
    public void handle(Transport<Object> t, String[] message, byte[][] rawMessage) {
        if (message.length <= 0 || message[0] == null) {
            replyError(t, "Unsupported COMMAND");
            return;
        }

        CommandHandler handler = get(message[0]);
        if (handler == null) {
            replyError(t, "Unsupported COMMAND");
            return;
        }

        handler.handle(t, message, rawMessage);
    }

    public void register(String name, CommandHandler handler) {
        handlers.put(name.toLowerCase(), handler);
    }

    public CommandHandler get(String name) {
        return handlers.get(name.toLowerCase());
    }
}

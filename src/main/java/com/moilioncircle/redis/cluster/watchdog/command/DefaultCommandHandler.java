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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author Leon Chen
 * @since 1.0.0
 */
public class DefaultCommandHandler extends AbstractCommandHandler {

    private Map<String, CommandHandler> handlers = new ConcurrentHashMap<>();
    public CommandHandler get(String name) { return handlers.get(name.toLowerCase()); }

    public CommandHandler addCommandHandler(String name, CommandHandler handler) {
        return handlers.put(name.toLowerCase(), handler);
    }

    public DefaultCommandHandler(ClusterManagers managers) {
        super(managers);
        addCommandHandler("ping", new PingCommandHandler(managers));
        addCommandHandler("info", new InfoCommandHandler(managers));
        addCommandHandler("dump", new DumpCommandHandler(managers));
        addCommandHandler("dbsize", new DBSizeCommandHandler(managers));
        addCommandHandler("config", new ConfigCommandHandler(managers));
        addCommandHandler("select", new SelectCommandHandler(managers));
        addCommandHandler("cluster", new ClusterCommandHandler(managers));
        addCommandHandler("restore", new RestoreCommandHandler(managers));
        addCommandHandler("shutdown", new ShutdownCommandHandler(managers));
        addCommandHandler("readonly", new ReadonlyCommandHandler(managers));
        addCommandHandler("readwrite", new ReadWriteCommandHandler(managers));
        addCommandHandler("restore-asking", new RestoreCommandHandler(managers));
    }

    @Override
    public void handle(Transport<byte[][]> t, String[] message, byte[][] rawMessage) {
        if (message.length <= 0 || message[0] == null) {
            replyError(t, "ERR Unsupported COMMAND"); return;
        }
        CommandHandler handler = get(message[0]);
        if (handler == null) {
            replyError(t, "ERR Unsupported COMMAND"); return;
        }
        handler.handle(t, message, rawMessage);
    }

}

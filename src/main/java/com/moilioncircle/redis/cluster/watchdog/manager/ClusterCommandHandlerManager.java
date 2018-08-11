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

package com.moilioncircle.redis.cluster.watchdog.manager;

import com.moilioncircle.redis.cluster.watchdog.command.CommandHandler;
import com.moilioncircle.redis.cluster.watchdog.command.DefaultCommandHandler;
import com.moilioncircle.redis.cluster.watchdog.storage.DefaultStorageEngine;
import com.moilioncircle.redis.cluster.watchdog.util.net.transport.Transport;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import static java.util.Arrays.stream;

/**
 * @author Leon Chen
 * @since 1.0.0
 */
public class ClusterCommandHandlerManager {
    
    private static final Log logger = LogFactory.getLog(ClusterCommandHandlerManager.class);
    
    private ClusterManagers managers;
    private DefaultCommandHandler handler;
    
    public ClusterCommandHandlerManager(ClusterManagers managers) {
        this.managers = managers;
        this.handler = new DefaultCommandHandler(managers);
    }
    
    public CommandHandler addCommandHandler(String name, CommandHandler handler) {
        if (managers.engine instanceof DefaultStorageEngine) {
            logger.warn("Using default storage engine. [ ClusterWatchdog.setStorageEngine(engine); ] first");
        }
        handler.setStorageEngine(managers.engine);
        handler.setConfiguration(managers.configuration);
        return this.handler.addCommandHandler(name, handler);
    }
    
    public void handleCommand(Transport<byte[][]> t, byte[][] raw) {
        this.handler.handle(t, stream(raw).map(String::new).toArray(String[]::new), raw);
    }
    
}

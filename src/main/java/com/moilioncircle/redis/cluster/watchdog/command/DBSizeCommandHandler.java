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

/**
 * @author Leon Chen
 * @since 1.0.0
 */
public class DBSizeCommandHandler extends AbstractCommandHandler {
    
    public DBSizeCommandHandler(ClusterManagers managers) {
        super(managers);
    }
    
    @Override
    public void handle(Transport<byte[][]> t, String[] message, byte[][] rawMessage) {
        if (message.length == 1) replyNumber(t, managers.engine.size());
        else replyError(t, "ERR wrong number of arguments for 'dbsize' command");
    }
}

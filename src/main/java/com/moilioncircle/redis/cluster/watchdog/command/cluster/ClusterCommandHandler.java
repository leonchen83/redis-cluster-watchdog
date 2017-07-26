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
import com.moilioncircle.redis.cluster.watchdog.command.CommandHandler;
import com.moilioncircle.redis.cluster.watchdog.manager.ClusterManagers;
import com.moilioncircle.redis.cluster.watchdog.util.net.transport.Transport;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Leon Chen
 * @since 1.0.0
 */
public class ClusterCommandHandler extends AbstractCommandHandler {

    private Map<String, CommandHandler> clusterHandlers = new HashMap<>();

    public ClusterCommandHandler(ClusterManagers managers) {
        super(managers);
        register("meet", new ClusterMeetCommandHandler(managers));
        register("myid", new ClusterMyIDCommandHandler(managers));
        register("info", new ClusterInfoCommandHandler(managers));
        register("nodes", new ClusterNodesCommandHandler(managers));
        register("slots", new ClusterSlotsCommandHandler(managers));
        register("reset", new ClusterResetCommandHandler(managers));
        register("forget", new ClusterForgetCommandHandler(managers));
        register("slaves", new ClusterSlavesCommandHandler(managers));
        register("keyslot", new ClusterKeySlotCommandHandler(managers));
        register("setslot", new ClusterSetSlotCommandHandler(managers));
        register("addslots", new ClusterAddSlotsCommandHandler(managers));
        register("delslots", new ClusterDelSlotsCommandHandler(managers));
        register("bumpepoch", new ClusterBumpEpochCommandHandler(managers));
        register("replicate", new ClusterReplicateCommandHandler(managers));
        register("saveconfig", new ClusterSaveConfigCommandHandler(managers));
        register("flushslots", new ClusterFlushSlotsCommandHandler(managers));
        register("getkeysinslot", new ClusterGetKeysInSlotCommandHandler(managers));
        register("countkeysinslot", new ClusterCountKeysInSlotCommandHandler(managers));
        register("set-config-epoch", new ClusterSetConfigEpochCommandHandler(managers));
        register("count-failure-reports", new ClusterCountFailureReportsCommandHandler(managers));
    }

    public void handle(Transport<Object> t, String[] message, byte[][] rawMessage) {
        if (message.length < 2 || message[1] == null) {
            replyError(t, "Wrong CLUSTER subcommand or number of arguments");
            return;
        }

        CommandHandler handler = get(message[1]);
        if (handler == null) {
            replyError(t, "Wrong CLUSTER subcommand or number of arguments");
            return;
        }

        handler.handle(t, message, rawMessage);
    }

    public void register(String name, CommandHandler handler) {
        clusterHandlers.put(name.toLowerCase(), handler);
    }

    public CommandHandler get(String name) {
        return clusterHandlers.get(name.toLowerCase());
    }
}

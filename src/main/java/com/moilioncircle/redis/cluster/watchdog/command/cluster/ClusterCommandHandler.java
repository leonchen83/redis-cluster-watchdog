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

import com.moilioncircle.redis.cluster.watchdog.ClusterConfigInfo;
import com.moilioncircle.redis.cluster.watchdog.command.AbstractCommandHandler;
import com.moilioncircle.redis.cluster.watchdog.command.CommandHandler;
import com.moilioncircle.redis.cluster.watchdog.manager.ClusterManagers;
import com.moilioncircle.redis.cluster.watchdog.util.net.transport.Transport;

import java.util.HashMap;
import java.util.Map;

import static com.moilioncircle.redis.cluster.watchdog.ClusterConfigInfo.valueOf;

/**
 * @author Leon Chen
 * @since 1.0.0
 */
public class ClusterCommandHandler extends AbstractCommandHandler {

    private Map<String, CommandHandler> clusterHandlers = new HashMap<>();
    public CommandHandler get(String name) { return clusterHandlers.get(name.toLowerCase()); }

    public CommandHandler addCommandHandler(String name, CommandHandler handler) {
        return clusterHandlers.put(name.toLowerCase(), handler);
    }

    public ClusterCommandHandler(ClusterManagers managers) {
        super(managers);
        addCommandHandler("meet", new ClusterMeetCommandHandler(managers));
        addCommandHandler("myid", new ClusterMyIDCommandHandler(managers));
        addCommandHandler("info", new ClusterInfoCommandHandler(managers));
        addCommandHandler("nodes", new ClusterNodesCommandHandler(managers));
        addCommandHandler("slots", new ClusterSlotsCommandHandler(managers));
        addCommandHandler("reset", new ClusterResetCommandHandler(managers));
        addCommandHandler("forget", new ClusterForgetCommandHandler(managers));
        addCommandHandler("slaves", new ClusterSlavesCommandHandler(managers));
        addCommandHandler("keyslot", new ClusterKeySlotCommandHandler(managers));
        addCommandHandler("setslot", new ClusterSetSlotCommandHandler(managers));
        addCommandHandler("addslots", new ClusterAddSlotsCommandHandler(managers));
        addCommandHandler("delslots", new ClusterDelSlotsCommandHandler(managers));
        addCommandHandler("bumpepoch", new ClusterBumpEpochCommandHandler(managers));
        addCommandHandler("replicate", new ClusterReplicateCommandHandler(managers));
        addCommandHandler("saveconfig", new ClusterSaveConfigCommandHandler(managers));
        addCommandHandler("flushslots", new ClusterFlushSlotsCommandHandler(managers));
        addCommandHandler("getkeysinslot", new ClusterGetKeysInSlotCommandHandler(managers));
        addCommandHandler("countkeysinslot", new ClusterCountKeysInSlotCommandHandler(managers));
        addCommandHandler("set-config-epoch", new ClusterSetConfigEpochCommandHandler(managers));
        addCommandHandler("count-failure-reports", new ClusterCountFailureReportsCommandHandler(managers));
    }

    public void handle(Transport<byte[][]> t, String[] message, byte[][] rawMessage) {
        if (message.length < 2 || message[1] == null) {
            replyError(t, "ERR Wrong CLUSTER subcommand or number of arguments"); return;
        }

        CommandHandler handler = get(message[1]);
        if (handler == null) {
            replyError(t, "ERR Wrong CLUSTER subcommand or number of arguments"); return;
        }
        managers.cron.execute(() -> {
            ClusterConfigInfo previous;
            previous = valueOf(managers.server.cluster);
            handler.handle(t, message, rawMessage);
            ClusterConfigInfo next = valueOf(managers.server.cluster);
            if (!previous.equals(next)) managers.config.submit(() -> managers.configs.clusterSaveConfig(next));
        });
    }
}

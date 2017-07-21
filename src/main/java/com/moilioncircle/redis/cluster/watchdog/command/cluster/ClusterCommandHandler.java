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
        register("slaves", new ClusterSlavesCommandHandler(managers));
        register("setslot", new ClusterSetSlotCommandHandler(managers));
        register("addslots", new ClusterAddSlotsCommandHandler(managers));
        register("delslots", new ClusterDelSlotsCommandHandler(managers));
        register("bumpepoch", new ClusterBumpEpochCommandHandler(managers));
        register("replicate", new ClusterReplicateCommandHandler(managers));
        register("flushslots", new ClusterFlushSlotsCommandHandler(managers));
        register("countkeysinslot", new ClusterCountKeyInSlotCommandHandler(managers));
        register("set-config-epoch", new ClusterSetConfigEpochCommandHandler(managers));
        register("count-failure-reports", new ClusterCountFailureReportsCommandHandler(managers));
    }

    public void handle(Transport<Object> t, String[] message, byte[][] rawMessage) {
        if ((message.length == 4 || message.length == 5) && message[1].equalsIgnoreCase("meet")) {
            get("meet").handle(t, message, rawMessage);
        } else if (message.length == 2 && message[1].equalsIgnoreCase("nodes")) {
            get("nodes").handle(t, message, rawMessage);
        } else if (message.length == 2 && message[1].equalsIgnoreCase("myid")) {
            get("myid").handle(t, message, rawMessage);
        } else if (message.length == 2 && message[1].equalsIgnoreCase("slots")) {
            get("slots").handle(t, message, rawMessage);
        } else if (message.length == 2 && message[1].equalsIgnoreCase("flushslots")) {
            get("flushslots").handle(t, message, rawMessage);
        } else if (message.length >= 3 && message[1].equalsIgnoreCase("addslots")) {
            get("addslots").handle(t, message, rawMessage);
        } else if (message.length >= 3 && message[1].equalsIgnoreCase("delslots")) {
            get("delslots").handle(t, message, rawMessage);
        } else if (message.length >= 4 && message[1].equalsIgnoreCase("setslot")) {
            get("setslot").handle(t, message, rawMessage);
        } else if (message.length == 2 && message[1].equalsIgnoreCase("bumpepoch")) {
            get("bumpepoch").handle(t, message, rawMessage);
        } else if (message.length == 2 && message[1].equalsIgnoreCase("info")) {
            get("info").handle(t, message, rawMessage);
        } else if (message.length == 2 && message[1].equalsIgnoreCase("saveconfig")) {
            get("saveconfig").handle(t, message, rawMessage);
        } else if (message.length == 3 && message[1].equalsIgnoreCase("keyslot")) {
            get("keyslot").handle(t, message, rawMessage);
        } else if (message.length == 3 && message[1].equalsIgnoreCase("countkeysinslot")) {
            get("countkeysinslot").handle(t, message, rawMessage);
        } else if (message.length == 3 && message[1].equalsIgnoreCase("forget")) {
            get("forget").handle(t, message, rawMessage);
        } else if (message.length == 3 && message[1].equalsIgnoreCase("replicate")) {
            get("replicate").handle(t, message, rawMessage);
        } else if (message.length == 3 && message[1].equalsIgnoreCase("slaves")) {
            get("slaves").handle(t, message, rawMessage);
        } else if (message.length == 3 && message[1].equalsIgnoreCase("count-failure-reports")) {
            get("count-failure-reports").handle(t, message, rawMessage);
        } else if (message.length == 3 && message[1].equalsIgnoreCase("set-config-epoch")) {
            get("set-config-epoch").handle(t, message, rawMessage);
        } else if ((message.length == 2 || message.length == 3) && message[1].equalsIgnoreCase("reset")) {
            get("reset").handle(t, message, rawMessage);
        } else {
            t.write(("-ERR Wrong CLUSTER subcommand or number of arguments\r\n").getBytes(), true);
        }
    }

    public void register(String name, CommandHandler handler) {
        clusterHandlers.put(name, handler);
    }

    public CommandHandler get(String name) {
        return clusterHandlers.get(name);
    }
}

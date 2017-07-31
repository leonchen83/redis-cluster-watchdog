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
import com.moilioncircle.redis.cluster.watchdog.manager.ClusterManagers;
import com.moilioncircle.redis.cluster.watchdog.util.net.transport.Transport;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import static java.lang.Long.parseLong;

/**
 * @author Leon Chen
 * @since 1.0.0
 */
public class ClusterSetConfigEpochCommandHandler extends AbstractCommandHandler {

    private static final Log logger = LogFactory.getLog(ClusterSetConfigEpochCommandHandler.class);

    public ClusterSetConfigEpochCommandHandler(ClusterManagers managers) {
        super(managers);
    }

    @Override
    public void handle(Transport<Object> t, String[] message, byte[][] rawMessage) {
        if (message.length != 3) {
            replyError(t, "Wrong CLUSTER subcommand or number of arguments"); return;
        }

        long epoch;
        try { epoch = parseLong(message[2]); }
        catch (Exception e) { replyError(t, "Invalid config epoch specified: " + message[2]); return; }

        if (epoch < 0) {
            replyError(t, "Invalid config epoch specified: " + epoch);
        } else if (server.myself.configEpoch != 0) {
            replyError(t, "Node config epoch is already non-zero");
        } else if (server.cluster.nodes.size() > 1) {
            replyError(t, "The user can assign a config epoch only when the node does not know any other node.");
        } else {
            server.myself.configEpoch = epoch;
            logger.info("configEpoch set to " + server.myself.configEpoch + " via CLUSTER SET-CONFIG-EPOCH");
            if (server.cluster.currentEpoch < epoch) server.cluster.currentEpoch = epoch;
            managers.states.clusterUpdateState(); reply(t, "OK");
        }
    }
}

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

import com.moilioncircle.redis.cluster.watchdog.ClusterConfiguration;
import com.moilioncircle.redis.cluster.watchdog.state.ClusterNode;
import com.moilioncircle.redis.cluster.watchdog.state.ServerState;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import static com.moilioncircle.redis.cluster.watchdog.ClusterConfigInfo.valueOf;
import static com.moilioncircle.redis.cluster.watchdog.ClusterConstants.*;
import static com.moilioncircle.redis.cluster.watchdog.manager.ClusterSlotManger.bitmapTestBit;
import static com.moilioncircle.redis.cluster.watchdog.state.NodeStates.nodeFailed;
import static com.moilioncircle.redis.cluster.watchdog.state.NodeStates.nodeIsMaster;
import static java.util.concurrent.ThreadLocalRandom.current;

/**
 * @author Leon Chen
 * @since 1.0.0
 */
public class ClusterFailoverManager {

    private static final Log logger = LogFactory.getLog(ClusterFailoverManager.class);

    private ServerState server;
    private ClusterManagers managers;
    private ClusterConfiguration configuration;

    public ClusterFailoverManager(ClusterManagers managers) {
        this.managers = managers;
        this.server = managers.server;
        this.configuration = managers.configuration;
    }

    public void clusterHandleSlaveFailover() {
        if (!configuration.isMaster()) return;
        if (nodeIsMaster(server.myself)) return;
        if (server.myself.master == null) return;
        if (!nodeFailed(server.myself.master)) return;
        if (server.myself.master.assignedSlots == 0) return;

        long now = System.currentTimeMillis();
        long authAge = now - server.cluster.failoverAuthTime;
        long authTimeout = Math.max(configuration.getClusterNodeTimeout() * 2, 2000);
        long authRetryTime = authTimeout * 2; int quorum = (server.cluster.size / 2) + 1;

        if (authAge > authRetryTime) {
            server.cluster.failoverAuthTime = now + 500 + current().nextInt(500);
            server.cluster.failoverAuthRank = managers.nodes.clusterGetSlaveRank();
            server.cluster.failoverAuthTime += server.cluster.failoverAuthRank * 1000;
            server.cluster.failoverAuthCount = 0; server.cluster.failoverAuthSent = false;
            managers.messages.clusterBroadcastPong(CLUSTER_BROADCAST_LOCAL_SLAVES); return;
        }

        int rank;
        if (!server.cluster.failoverAuthSent && (rank = managers.nodes.clusterGetSlaveRank()) > server.cluster.failoverAuthRank) {
            long delay = (rank - server.cluster.failoverAuthRank) * 1000;
            server.cluster.failoverAuthTime += delay; server.cluster.failoverAuthRank = rank;
            logger.info("Slave rank updated to #" + rank + ", added " + delay + " milliseconds of delay.");
        }

        now = System.currentTimeMillis();
        if (now < server.cluster.failoverAuthTime) return;
        if (authAge > authTimeout) return;

        if (!server.cluster.failoverAuthSent) {
            server.cluster.currentEpoch++;
            server.cluster.failoverAuthEpoch = server.cluster.currentEpoch;
            logger.info("Starting a failover election for epoch " + server.cluster.currentEpoch + ".");
            managers.messages.clusterRequestFailoverAuth();
            server.cluster.failoverAuthSent = true; return;
        }

        if (server.cluster.failoverAuthCount >= quorum) {
            logger.info("Failover election won: I'm the new master.");
            if (server.myself.configEpoch < server.cluster.failoverAuthEpoch) {
                server.myself.configEpoch = server.cluster.failoverAuthEpoch;
                logger.info("configEpoch set to " + server.myself.configEpoch + " after successful failover");
            }
            clusterFailoverReplaceMyMaster();
        }
    }

    public void clusterFailoverReplaceMyMaster() {
        ClusterNode myself = server.myself;
        if (nodeIsMaster(myself)) return;
        if (myself.master == null) return;
        ClusterNode previous = myself.master;
        managers.nodes.clusterSetNodeAsMaster(myself);
        managers.replications.replicationUnsetMaster();
        for (int i = 0; i < CLUSTER_SLOTS; i++) {
            if (!bitmapTestBit(previous.slots, i)) continue;
            managers.slots.clusterDelSlot(i);
            managers.slots.clusterAddSlot(myself, i);
        }
        managers.states.clusterUpdateState();
        managers.configs.clusterSaveConfig(valueOf(server.cluster));
        managers.messages.clusterBroadcastPong(CLUSTER_BROADCAST_ALL);
    }
}

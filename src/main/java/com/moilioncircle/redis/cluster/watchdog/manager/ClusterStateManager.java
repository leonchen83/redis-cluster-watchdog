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

import com.moilioncircle.redis.cluster.watchdog.ClusterConfiguration;
import com.moilioncircle.redis.cluster.watchdog.ClusterState;
import com.moilioncircle.redis.cluster.watchdog.state.ClusterNode;
import com.moilioncircle.redis.cluster.watchdog.state.ServerState;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import static com.moilioncircle.redis.cluster.watchdog.ClusterConstants.CLUSTER_MAX_REJOIN_DELAY;
import static com.moilioncircle.redis.cluster.watchdog.ClusterConstants.CLUSTER_MIN_REJOIN_DELAY;
import static com.moilioncircle.redis.cluster.watchdog.ClusterConstants.CLUSTER_SLOTS;
import static com.moilioncircle.redis.cluster.watchdog.ClusterConstants.CLUSTER_WRITABLE_DELAY;
import static com.moilioncircle.redis.cluster.watchdog.ClusterState.CLUSTER_FAIL;
import static com.moilioncircle.redis.cluster.watchdog.ClusterState.CLUSTER_OK;
import static com.moilioncircle.redis.cluster.watchdog.state.NodeStates.nodeFailed;
import static com.moilioncircle.redis.cluster.watchdog.state.NodeStates.nodeIsMaster;
import static com.moilioncircle.redis.cluster.watchdog.state.NodeStates.nodePFailed;
import static java.lang.Math.max;
import static java.lang.Math.min;

/**
 * @author Leon Chen
 * @since 1.0.0
 */
public class ClusterStateManager {
    
    private static final Log logger = LogFactory.getLog(ClusterStateManager.class);
    
    private ServerState server;
    private ClusterManagers managers;
    private ClusterConfiguration configuration;
    
    public ClusterStateManager(ClusterManagers managers) {
        this.managers = managers;
        this.server = managers.server;
        this.configuration = managers.configuration;
    }
    
    public boolean clusterBumpConfigEpochWithoutConsensus() {
        long max = managers.nodes.clusterGetMaxEpoch();
        if (server.myself.configEpoch == 0 || server.myself.configEpoch != max) {
            server.cluster.currentEpoch++;
            server.myself.configEpoch = server.cluster.currentEpoch;
            logger.info("New config epoch was set to : " + server.myself.configEpoch);
            return true;
        }
        return false;
    }
    
    public void clusterUpdateState() {
        long now = System.currentTimeMillis();
        if (server.stateSaveTime == 0) server.stateSaveTime = now;
        if (nodeIsMaster(server.myself) && server.cluster.state == CLUSTER_FAIL
                && now - server.stateSaveTime < CLUSTER_WRITABLE_DELAY) return;
        //
        ClusterState state = CLUSTER_OK;
        if (configuration.isClusterFullCoverage()) {
            for (int i = 0; i < CLUSTER_SLOTS; i++) {
                ClusterNode n = server.cluster.slots[i];
                if (n == null || nodeFailed(n.flags)) {
                    state = CLUSTER_FAIL;
                    break;
                }
            }
        }
        //
        int masters = 0;
        server.cluster.size = 0;
        for (ClusterNode node : server.cluster.nodes.values()) {
            if (nodeIsMaster(node) && node.assignedSlots != 0) {
                server.cluster.size++;
                if (!nodeFailed(node.flags) && !nodePFailed(node.flags)) masters++;
            }
        }
        //
        int quorum = (server.cluster.size / 2) + 1;
        if (masters < quorum) {
            state = CLUSTER_FAIL;
            server.amongMinorityTime = now;
        }
        //
        if (state != server.cluster.state) {
            long timeout = configuration.getClusterNodeTimeout();
            long rejoin = max(min(timeout, CLUSTER_MAX_REJOIN_DELAY), CLUSTER_MIN_REJOIN_DELAY);
            if (state == CLUSTER_OK && nodeIsMaster(server.myself) && now - server.amongMinorityTime < rejoin) return;
            logger.info("Cluster state changed: " + state.getDisplay());
            server.cluster.state = state;
            managers.notifyStateChanged(state);
        }
    }
}

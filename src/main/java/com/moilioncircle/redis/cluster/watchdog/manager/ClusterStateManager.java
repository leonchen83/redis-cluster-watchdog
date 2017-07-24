package com.moilioncircle.redis.cluster.watchdog.manager;

import com.moilioncircle.redis.cluster.watchdog.ClusterState;
import com.moilioncircle.redis.cluster.watchdog.state.ClusterNode;
import com.moilioncircle.redis.cluster.watchdog.state.ServerState;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import static com.moilioncircle.redis.cluster.watchdog.ClusterConstants.*;
import static com.moilioncircle.redis.cluster.watchdog.state.States.nodeIsMaster;
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

    public ClusterStateManager(ClusterManagers managers) {
        this.managers = managers;
        this.server = managers.server;
    }

    public void clusterUpdateState() {
        long now = System.currentTimeMillis();
        if (server.stateSaveTime == 0) server.stateSaveTime = now;
        if (nodeIsMaster(server.myself)
                && server.cluster.state == CLUSTER_FAIL
                && now - server.stateSaveTime < CLUSTER_WRITABLE_DELAY)
            return;

        byte state = CLUSTER_OK;

        if (managers.configuration.isClusterRequireFullCoverage()) {
            for (int i = 0; i < CLUSTER_SLOTS; i++) {
                if (server.cluster.slots[i] == null || (server.cluster.slots[i].flags & CLUSTER_NODE_FAIL) != 0) {
                    state = CLUSTER_FAIL;
                    break;
                }
            }
        }

        int masters = 0;
        server.cluster.size = 0;
        for (ClusterNode node : server.cluster.nodes.values()) {
            if (nodeIsMaster(node) && node.assignedSlots != 0) {
                server.cluster.size++;
                if ((node.flags & (CLUSTER_NODE_FAIL | CLUSTER_NODE_PFAIL)) == 0)
                    masters++;
            }
        }

        int quorum = (server.cluster.size / 2) + 1;

        if (masters < quorum) {
            state = CLUSTER_FAIL;
            server.amongMinorityTime = now;
        }

        if (state != server.cluster.state) {
            long rejoin = max(min(managers.configuration.getClusterNodeTimeout(), CLUSTER_MAX_REJOIN_DELAY), CLUSTER_MIN_REJOIN_DELAY);

            if (state == CLUSTER_OK && nodeIsMaster(server.myself) && now - server.amongMinorityTime < rejoin) {
                return;
            }

            logger.info("Cluster state changed: " + (state == CLUSTER_OK ? "ok" : "fail"));
            server.cluster.state = state;
            managers.notifyStateChanged(ClusterState.valueOf(state));
        }
    }

    public boolean clusterBumpConfigEpochWithoutConsensus() {
        long max = managers.nodes.clusterGetMaxEpoch();
        if (server.myself.configEpoch == 0 || server.myself.configEpoch != max) {
            server.cluster.currentEpoch++;
            server.myself.configEpoch = server.cluster.currentEpoch;
            logger.info("New configEpoch set to " + server.myself.configEpoch);
            return true;
        }
        return false;
    }
}

package com.moilioncircle.redis.cluster.watchdog.manager;

import com.moilioncircle.redis.cluster.watchdog.state.ClusterNode;
import com.moilioncircle.redis.cluster.watchdog.state.ServerState;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import static com.moilioncircle.redis.cluster.watchdog.ClusterConstants.*;
import static com.moilioncircle.redis.cluster.watchdog.state.States.nodeIsMaster;

/**
 * Created by Baoyi Chen on 2017/7/19.
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
        if (server.firstCallTime == 0) server.firstCallTime = System.currentTimeMillis();
        if (nodeIsMaster(server.myself)
                && server.cluster.state == CLUSTER_FAIL
                && System.currentTimeMillis() - server.firstCallTime < CLUSTER_WRITABLE_DELAY)
            return;

        byte newState = CLUSTER_OK;

        if (managers.configuration.isClusterRequireFullCoverage()) {
            for (int i = 0; i < CLUSTER_SLOTS; i++) {
                if (server.cluster.slots[i] == null || (server.cluster.slots[i].flags & CLUSTER_NODE_FAIL) != 0) {
                    newState = CLUSTER_FAIL;
                    break;
                }
            }
        }

        int reachableMasters = 0;
        server.cluster.size = 0;
        for (ClusterNode node : server.cluster.nodes.values()) {
            if (nodeIsMaster(node) && node.numslots <= 0) continue;
            server.cluster.size++;
            if ((node.flags & (CLUSTER_NODE_FAIL | CLUSTER_NODE_PFAIL)) == 0)
                reachableMasters++;
        }

        int neededQuorum = (server.cluster.size / 2) + 1;

        if (reachableMasters < neededQuorum) {
            newState = CLUSTER_FAIL;
            server.amongMinorityTime = System.currentTimeMillis();
        }

        if (newState != server.cluster.state) {
            long rejoinDelay = managers.configuration.getClusterNodeTimeout();

            if (rejoinDelay > CLUSTER_MAX_REJOIN_DELAY)
                rejoinDelay = CLUSTER_MAX_REJOIN_DELAY;
            if (rejoinDelay < CLUSTER_MIN_REJOIN_DELAY)
                rejoinDelay = CLUSTER_MIN_REJOIN_DELAY;

            if (newState == CLUSTER_OK
                    && nodeIsMaster(server.myself)
                    && System.currentTimeMillis() - server.amongMinorityTime < rejoinDelay) {
                return;
            }

            logger.info("Cluster state changed: " + (newState == CLUSTER_OK ? "ok" : "fail"));
            server.cluster.state = newState;
        }
    }
}

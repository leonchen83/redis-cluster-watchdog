package com.moilioncircle.redis.cluster.watchdog.manager;

import com.moilioncircle.redis.cluster.watchdog.ClusterConstants;
import com.moilioncircle.redis.cluster.watchdog.state.ClusterNode;
import com.moilioncircle.redis.cluster.watchdog.state.ServerState;
import com.moilioncircle.redis.cluster.watchdog.state.States;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

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
        if (States.nodeIsMaster(server.myself)
                && server.cluster.state == ClusterConstants.CLUSTER_FAIL
                && System.currentTimeMillis() - server.firstCallTime < ClusterConstants.CLUSTER_WRITABLE_DELAY)
            return;

        byte newState = ClusterConstants.CLUSTER_OK;

        if (managers.configuration.isClusterRequireFullCoverage()) {
            for (int i = 0; i < ClusterConstants.CLUSTER_SLOTS; i++) {
                if (server.cluster.slots[i] == null || (server.cluster.slots[i].flags & ClusterConstants.CLUSTER_NODE_FAIL) != 0) {
                    newState = ClusterConstants.CLUSTER_FAIL;
                    break;
                }
            }
        }

        int reachableMasters = 0;
        server.cluster.size = 0;
        for (ClusterNode node : server.cluster.nodes.values()) {
            if (States.nodeIsMaster(node) && node.numslots <= 0) continue;
            server.cluster.size++;
            if ((node.flags & (ClusterConstants.CLUSTER_NODE_FAIL | ClusterConstants.CLUSTER_NODE_PFAIL)) == 0)
                reachableMasters++;
        }

        int neededQuorum = (server.cluster.size / 2) + 1;

        if (reachableMasters < neededQuorum) {
            newState = ClusterConstants.CLUSTER_FAIL;
            server.amongMinorityTime = System.currentTimeMillis();
        }

        if (newState != server.cluster.state) {
            long rejoinDelay = managers.configuration.getClusterNodeTimeout();

            if (rejoinDelay > ClusterConstants.CLUSTER_MAX_REJOIN_DELAY)
                rejoinDelay = ClusterConstants.CLUSTER_MAX_REJOIN_DELAY;
            if (rejoinDelay < ClusterConstants.CLUSTER_MIN_REJOIN_DELAY)
                rejoinDelay = ClusterConstants.CLUSTER_MIN_REJOIN_DELAY;

            if (newState == ClusterConstants.CLUSTER_OK
                    && States.nodeIsMaster(server.myself)
                    && System.currentTimeMillis() - server.amongMinorityTime < rejoinDelay) {
                return;
            }

            logger.info("Cluster state changed: " + (newState == ClusterConstants.CLUSTER_OK ? "ok" : "fail"));
            server.cluster.state = newState;
        }
    }
}

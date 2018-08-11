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

package com.moilioncircle.redis.cluster.watchdog.state;

import static com.moilioncircle.redis.cluster.watchdog.ClusterConstants.CLUSTER_NODE_FAIL;
import static com.moilioncircle.redis.cluster.watchdog.ClusterConstants.CLUSTER_NODE_HANDSHAKE;
import static com.moilioncircle.redis.cluster.watchdog.ClusterConstants.CLUSTER_NODE_MASTER;
import static com.moilioncircle.redis.cluster.watchdog.ClusterConstants.CLUSTER_NODE_MEET;
import static com.moilioncircle.redis.cluster.watchdog.ClusterConstants.CLUSTER_NODE_MIGRATE_TO;
import static com.moilioncircle.redis.cluster.watchdog.ClusterConstants.CLUSTER_NODE_MYSELF;
import static com.moilioncircle.redis.cluster.watchdog.ClusterConstants.CLUSTER_NODE_NOADDR;
import static com.moilioncircle.redis.cluster.watchdog.ClusterConstants.CLUSTER_NODE_PFAIL;
import static com.moilioncircle.redis.cluster.watchdog.ClusterConstants.CLUSTER_NODE_SLAVE;

/**
 * @author Leon Chen
 * @since 1.0.0
 */
public class NodeStates {
    
    /**
     *
     */
    public static boolean nodeFailed(ClusterNode n) {
        return nodeFailed(n.flags);
    }
    
    public static boolean nodeIsSlave(ClusterNode n) {
        return nodeIsSlave(n.flags);
    }
    
    public static boolean nodePFailed(ClusterNode n) {
        return nodePFailed(n.flags);
    }
    
    public static boolean nodeHasAddr(ClusterNode n) {
        return nodeHasAddr(n.flags);
    }
    
    public static boolean nodeIsMaster(ClusterNode n) {
        return nodeIsMaster(n.flags);
    }
    
    public static boolean nodeInHandshake(ClusterNode n) {
        return nodeInHandshake(n.flags);
    }
    
    public static boolean nodeWithoutAddr(ClusterNode n) {
        return nodeWithoutAddr(n.flags);
    }
    
    /**
     *
     */
    public static boolean nodeFailed(int flags) {
        return (flags & CLUSTER_NODE_FAIL) != 0;
    }
    
    public static boolean nodeInMeet(int flags) {
        return (flags & CLUSTER_NODE_MEET) != 0;
    }
    
    public static boolean nodePFailed(int flags) {
        return (flags & CLUSTER_NODE_PFAIL) != 0;
    }
    
    public static boolean nodeIsSlave(int flags) {
        return (flags & CLUSTER_NODE_SLAVE) != 0;
    }
    
    public static boolean nodeHasAddr(int flags) {
        return (flags & CLUSTER_NODE_NOADDR) == 0;
    }
    
    public static boolean nodeIsMaster(int flags) {
        return (flags & CLUSTER_NODE_MASTER) != 0;
    }
    
    public static boolean nodeIsMyself(int flags) {
        return (flags & CLUSTER_NODE_MYSELF) != 0;
    }
    
    public static boolean nodeWithoutAddr(int flags) {
        return (flags & CLUSTER_NODE_NOADDR) != 0;
    }
    
    public static boolean nodeInMigrate(int flags) {
        return (flags & CLUSTER_NODE_MIGRATE_TO) != 0;
    }
    
    public static boolean nodeInHandshake(int flags) {
        return (flags & CLUSTER_NODE_HANDSHAKE) != 0;
    }
}

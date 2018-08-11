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

import com.moilioncircle.redis.cluster.watchdog.state.ClusterNode;
import com.moilioncircle.redis.cluster.watchdog.state.ServerState;
import com.moilioncircle.redis.cluster.watchdog.util.type.Tuple2;

import java.util.Map;

import static com.moilioncircle.redis.cluster.watchdog.ClusterConstants.CLUSTER_BLACKLIST_TTL;
import static com.moilioncircle.redis.cluster.watchdog.util.Tuples.of;

/**
 * @author Leon Chen
 * @since 1.0.0
 */
public class ClusterBlacklistManager {
    
    private ServerState server;
    
    public ClusterBlacklistManager(ClusterManagers managers) {
        this.server = managers.server;
    }
    
    public boolean clusterBlacklistExists(String name) {
        long now = System.currentTimeMillis();
        Map<String, Tuple2<Long, ClusterNode>> map = server.cluster.blacklist;
        map.values().removeIf(e -> e.getV1() < now);
        return map.containsKey(name);
    }
    
    public void clusterBlacklistAddNode(ClusterNode node) {
        long now = System.currentTimeMillis();
        server.cluster.blacklist.values().removeIf(e -> e.getV1() < now);
        server.cluster.blacklist.put(node.name, of(now + CLUSTER_BLACKLIST_TTL, node));
    }
}

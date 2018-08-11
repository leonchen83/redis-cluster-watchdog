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

import com.moilioncircle.redis.cluster.watchdog.util.type.Tuple2;

import java.util.LinkedHashMap;
import java.util.Map;

import static com.moilioncircle.redis.cluster.watchdog.ClusterConstants.CLUSTERMSG_TYPE_COUNT;
import static com.moilioncircle.redis.cluster.watchdog.ClusterConstants.CLUSTER_SLOTS;
import static com.moilioncircle.redis.cluster.watchdog.ClusterState.CLUSTER_FAIL;

/**
 * @author Leon Chen
 * @since 1.0.0
 */
public class ClusterState {
    public int size = 1;
    public long pFailNodes = 0;
    public long currentEpoch = 0;
    public long lastVoteEpoch = 0;
    public ClusterNode[] slots = new ClusterNode[CLUSTER_SLOTS];
    public long[] messagesSent = new long[CLUSTERMSG_TYPE_COUNT];
    public Map<String, ClusterNode> nodes = new LinkedHashMap<>();
    public ClusterNode[] migrating = new ClusterNode[CLUSTER_SLOTS];
    public ClusterNode[] importing = new ClusterNode[CLUSTER_SLOTS];
    public long[] messagesReceived = new long[CLUSTERMSG_TYPE_COUNT];
    public int failoverAuthRank = 0;
    public ClusterNode myself = null;
    public long failoverAuthTime = 0;
    public int failoverAuthCount = 0;
    public long failoverAuthEpoch = 0;
    public boolean failoverAuthSent = false;
    public Map<String, Tuple2<Long, ClusterNode>> blacklist = new LinkedHashMap<>();
    public com.moilioncircle.redis.cluster.watchdog.ClusterState state = CLUSTER_FAIL;
}

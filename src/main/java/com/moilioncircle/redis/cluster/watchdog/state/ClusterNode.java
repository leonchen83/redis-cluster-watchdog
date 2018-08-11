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

import java.util.ArrayList;
import java.util.List;

import static com.moilioncircle.redis.cluster.watchdog.ClusterConstants.CLUSTER_SLOTS_BYTES;

/**
 * @author Leon Chen
 * @since 1.0.0
 */
public class ClusterNode {
    public long failTime;
    public long pingTime;
    public long pongTime;
    public long votedTime;
    public long createTime;
    public long isolatedTime;
    public long configEpoch;
    public int assignedSlots;
    public byte[] slots = new byte[CLUSTER_SLOTS_BYTES];
    public List<ClusterNode> slaves = new ArrayList<>();
    public String ip;
    public int port;
    public int busPort;
    public int flags;
    public String name;
    public long offset;
    public ClusterNode master;
    public volatile ClusterLink link;
    public List<ClusterNodeFailReport> failReports = new ArrayList<>();
    
    public ClusterNode() {
        this.createTime = System.currentTimeMillis();
    }
}

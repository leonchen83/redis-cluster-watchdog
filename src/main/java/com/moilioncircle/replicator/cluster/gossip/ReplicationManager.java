/*
 * Copyright 2016 leon chen
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

package com.moilioncircle.replicator.cluster.gossip;

import com.moilioncircle.replicator.cluster.ClusterNode;
import com.moilioncircle.replicator.cluster.Server;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * @author Leon Chen
 * @since 2.1.0
 */
public class ReplicationManager {
    private static final Log logger = LogFactory.getLog(ReplicationManager.class);
    private Server server;
    private ThinGossip gossip;

    public ReplicationManager(ThinGossip gossip) {
        this.gossip = gossip;
        this.server = gossip.server;
    }

    public void replicationSetMaster(ClusterNode node) {
        logger.info("replicate to " + node.ip + ":" + node.port);
        server.masterHost = node.ip;
        server.masterPort = node.port;
    }

    public long replicationGetSlaveOffset() {
        return 0;
    }
}

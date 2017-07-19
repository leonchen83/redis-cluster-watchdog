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

package com.moilioncircle.replicator.cluster.manager;

import com.moilioncircle.replicator.cluster.state.ClusterNode;
import com.moilioncircle.replicator.cluster.state.ServerState;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * @author Leon Chen
 * @since 2.1.0
 */
public class ReplicationManager {
    private static final Log logger = LogFactory.getLog(ReplicationManager.class);
    private ServerState server;
    private ClusterManagers managers;

    public ReplicationManager(ClusterManagers managers) {
        this.managers = managers;
        this.server = managers.server;
    }

    public void replicationSetMaster(ClusterNode node) {
        logger.info("replicate to " + node.ip + ":" + node.port);
        server.masterHost = node.ip;
        server.masterPort = node.port;
    }

    public void replicationUnsetMaster() {
        server.masterHost = null;
        server.masterPort = 0;
    }

    public long replicationGetSlaveOffset() {
        return 0;
    }
}
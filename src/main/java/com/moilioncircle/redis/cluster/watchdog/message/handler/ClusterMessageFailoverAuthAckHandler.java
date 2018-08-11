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

package com.moilioncircle.redis.cluster.watchdog.message.handler;

import com.moilioncircle.redis.cluster.watchdog.manager.ClusterManagers;
import com.moilioncircle.redis.cluster.watchdog.message.ClusterMessage;
import com.moilioncircle.redis.cluster.watchdog.state.ClusterLink;
import com.moilioncircle.redis.cluster.watchdog.state.ClusterNode;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import static com.moilioncircle.redis.cluster.watchdog.state.NodeStates.nodeIsMaster;

/**
 * @author Leon Chen
 * @since 1.0.0
 */
public class ClusterMessageFailoverAuthAckHandler extends AbstractClusterMessageHandler {
    
    private static final Log logger = LogFactory.getLog(ClusterMessageFailoverAuthAckHandler.class);
    
    public ClusterMessageFailoverAuthAckHandler(ClusterManagers managers) {
        super(managers);
    }
    
    @Override
    public boolean handle(ClusterNode sender, ClusterLink link, ClusterMessage hdr) {
        logger.debug("Failover auth ack packet received: node:" + (link.node == null ? "(nil)" : link.node.name));
        
        if (sender == null) return true;
        if (nodeIsMaster(sender) && sender.assignedSlots > 0 && hdr.currentEpoch >= server.cluster.failoverAuthEpoch) {
            server.cluster.failoverAuthCount++;
            managers.failovers.clusterHandleSlaveFailover();
        }
        return true;
    }
}

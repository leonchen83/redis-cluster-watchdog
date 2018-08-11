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

import static com.moilioncircle.redis.cluster.watchdog.ClusterConstants.CLUSTERMSG_FLAG0_FORCEACK;
import static com.moilioncircle.redis.cluster.watchdog.ClusterConstants.CLUSTER_SLOTS;
import static com.moilioncircle.redis.cluster.watchdog.manager.ClusterSlotManager.bitmapTestBit;
import static com.moilioncircle.redis.cluster.watchdog.state.NodeStates.nodeFailed;
import static com.moilioncircle.redis.cluster.watchdog.state.NodeStates.nodeIsMaster;
import static com.moilioncircle.redis.cluster.watchdog.state.NodeStates.nodeIsSlave;

/**
 * @author Leon Chen
 * @since 1.0.0
 */
public class ClusterMessageFailoverAuthRequestHandler extends AbstractClusterMessageHandler {
    
    private static final Log logger = LogFactory.getLog(ClusterMessageFailoverAuthRequestHandler.class);
    
    public ClusterMessageFailoverAuthRequestHandler(ClusterManagers managers) {
        super(managers);
    }
    
    @Override
    public boolean handle(ClusterNode sender, ClusterLink link, ClusterMessage hdr) {
        logger.debug("Failover auth request packet received: node:" + (link.node == null ? "(nil)" : link.node.name));
        if (sender == null) return true;
        clusterSendFailoverAuthIfNeeded(sender, hdr);
        return true;
    }
    
    public void clusterSendFailoverAuthIfNeeded(ClusterNode node, ClusterMessage hdr) {
        ClusterNode master = node.master;
        long now = System.currentTimeMillis();
        boolean force = (hdr.messageFlags[0] & CLUSTERMSG_FLAG0_FORCEACK) != 0;
        //
        if (nodeIsSlave(server.myself)) return;
        if (server.myself.assignedSlots == 0) return;
        if (hdr.currentEpoch < server.cluster.currentEpoch) return;
        if (server.cluster.lastVoteEpoch == server.cluster.currentEpoch) return;
        if (nodeIsMaster(node) || master == null || (!nodeFailed(master) && !force)) return;
        if (now - master.votedTime < managers.configuration.getClusterNodeTimeout() * 2) return;
        
        for (int i = 0; i < CLUSTER_SLOTS; i++) {
            if (!bitmapTestBit(hdr.slots, i)) continue;
            if (server.cluster.slots[i] == null) continue;
            if (server.cluster.slots[i].configEpoch <= hdr.configEpoch) continue;
            return;
        }
        
        managers.messages.clusterSendFailoverAuth(node);
        node.master.votedTime = System.currentTimeMillis();
        server.cluster.lastVoteEpoch = server.cluster.currentEpoch;
        logger.info("Failover auth granted to " + node.name + " for epoch " + server.cluster.currentEpoch);
    }
}

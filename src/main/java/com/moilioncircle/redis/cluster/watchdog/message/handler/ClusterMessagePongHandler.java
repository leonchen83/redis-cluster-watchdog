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

import java.util.Arrays;
import java.util.Objects;

import static com.moilioncircle.redis.cluster.watchdog.ClusterConstants.CLUSTER_FAIL_UNDO_TIME_MULTI;
import static com.moilioncircle.redis.cluster.watchdog.ClusterConstants.CLUSTER_NODE_FAIL;
import static com.moilioncircle.redis.cluster.watchdog.ClusterConstants.CLUSTER_NODE_HANDSHAKE;
import static com.moilioncircle.redis.cluster.watchdog.ClusterConstants.CLUSTER_NODE_MASTER;
import static com.moilioncircle.redis.cluster.watchdog.ClusterConstants.CLUSTER_NODE_MIGRATE_TO;
import static com.moilioncircle.redis.cluster.watchdog.ClusterConstants.CLUSTER_NODE_NOADDR;
import static com.moilioncircle.redis.cluster.watchdog.ClusterConstants.CLUSTER_NODE_PFAIL;
import static com.moilioncircle.redis.cluster.watchdog.ClusterConstants.CLUSTER_NODE_SLAVE;
import static com.moilioncircle.redis.cluster.watchdog.ClusterConstants.CLUSTER_SLOTS;
import static com.moilioncircle.redis.cluster.watchdog.ClusterNodeInfo.valueOf;
import static com.moilioncircle.redis.cluster.watchdog.manager.ClusterSlotManager.bitmapTestBit;
import static com.moilioncircle.redis.cluster.watchdog.state.NodeStates.nodeFailed;
import static com.moilioncircle.redis.cluster.watchdog.state.NodeStates.nodeInHandshake;
import static com.moilioncircle.redis.cluster.watchdog.state.NodeStates.nodeIsMaster;
import static com.moilioncircle.redis.cluster.watchdog.state.NodeStates.nodeIsSlave;
import static com.moilioncircle.redis.cluster.watchdog.state.NodeStates.nodePFailed;

/**
 * @author Leon Chen
 * @since 1.0.0
 */
public class ClusterMessagePongHandler extends AbstractClusterMessageHandler {
    
    private static final Log logger = LogFactory.getLog(ClusterMessagePongHandler.class);
    
    public ClusterMessagePongHandler(ClusterManagers managers) {
        super(managers);
    }
    
    @Override
    public boolean handle(ClusterNode sender, ClusterLink link, ClusterMessage hdr) {
        logger.debug("Pong packet received: node:" + (link.node == null ? "(nil)" : link.node.name));
        
        if (link.node != null) {
            if (nodeInHandshake(link.node)) {
                if (sender != null) {
                    nodeUpdateAddressIfNeeded(sender, link, hdr);
                    managers.nodes.clusterDelNode(link.node);
                    return false;
                }
                managers.nodes.clusterRenameNode(link.node, hdr.name);
                link.node.flags &= ~CLUSTER_NODE_HANDSHAKE;
                link.node.flags |= hdr.flags & (CLUSTER_NODE_MASTER | CLUSTER_NODE_SLAVE);
            } else if (!link.node.name.equals(hdr.name)) {
                link.node.flags |= CLUSTER_NODE_NOADDR;
                link.node.ip = null;
                link.node.port = 0;
                link.node.busPort = 0;
                managers.connections.freeClusterLink(link);
                return false;
            }
            
            link.node.pongTime = System.currentTimeMillis();
            link.node.pingTime = 0;
            
            if (nodePFailed(link.node)) {
                link.node.flags &= ~CLUSTER_NODE_PFAIL;
                managers.notifyUnsetNodePFailed(valueOf(link.node, server.myself));
            } else if (nodeFailed(link.node)) clearNodeFailureIfNeeded(link.node);
        }
        
        if (sender == null) return true;
        
        if (hdr.master == null) managers.nodes.clusterSetNodeAsMaster(sender);
        else {
            ClusterNode master = managers.nodes.clusterLookupNode(hdr.master);
            if (nodeIsMaster(sender)) {
                managers.slots.clusterDelNodeSlots(sender);
                sender.flags &= ~(CLUSTER_NODE_MASTER | CLUSTER_NODE_MIGRATE_TO);
                sender.flags |= CLUSTER_NODE_SLAVE;
            }
            if (master != null && (sender.master == null || !Objects.equals(sender.master, master))) {
                if (sender.master != null) managers.nodes.clusterNodeRemoveSlave(sender.master, sender);
                managers.nodes.clusterNodeAddSlave(master, sender);
                sender.master = master;
            }
        }
        
        ClusterNode senderMaster = nodeIsMaster(sender) ? sender : sender.master;
        if (senderMaster != null && !Arrays.equals(senderMaster.slots, hdr.slots)) {
            if (nodeIsMaster(sender)) clusterUpdateSlotsConfigWith(sender, hdr.configEpoch, hdr.slots);
            
            for (int i = 0; i < CLUSTER_SLOTS; i++) {
                if (!bitmapTestBit(hdr.slots, i)) continue;
                if (server.cluster.slots[i] == null) continue;
                if (Objects.equals(server.cluster.slots[i], sender)) continue;
                if (server.cluster.slots[i].configEpoch <= hdr.configEpoch) continue;
                managers.messages.clusterSendUpdate(sender.link, server.cluster.slots[i]);
                break;
            }
        }
        
        if (nodeIsMaster(server.myself) && nodeIsMaster(sender) && hdr.configEpoch == server.myself.configEpoch)
            clusterHandleConfigEpochCollision(sender);
        clusterProcessGossipSection(hdr, link);
        return true;
    }
    
    public void clearNodeFailureIfNeeded(ClusterNode node) {
        long now = System.currentTimeMillis();
        long timeout = managers.configuration.getClusterNodeTimeout() * CLUSTER_FAIL_UNDO_TIME_MULTI;
        
        if (nodeIsSlave(node) || node.assignedSlots == 0) {
            node.flags &= ~CLUSTER_NODE_FAIL;
            managers.notifyUnsetNodeFailed(valueOf(node, server.myself));
        }
        if (nodeIsMaster(node) && node.assignedSlots > 0 && now - node.failTime > timeout) {
            node.flags &= ~CLUSTER_NODE_FAIL;
            managers.notifyUnsetNodeFailed(valueOf(node, server.myself));
        }
    }
}

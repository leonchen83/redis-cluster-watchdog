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

import static com.moilioncircle.redis.cluster.watchdog.ClusterConstants.CLUSTER_NODE_FAIL;
import static com.moilioncircle.redis.cluster.watchdog.ClusterConstants.CLUSTER_NODE_PFAIL;
import static com.moilioncircle.redis.cluster.watchdog.ClusterNodeInfo.valueOf;
import static com.moilioncircle.redis.cluster.watchdog.state.NodeStates.nodeFailed;
import static com.moilioncircle.redis.cluster.watchdog.state.NodeStates.nodeIsMyself;

/**
 * @author Leon Chen
 * @since 1.0.0
 */
public class ClusterMessageFailHandler extends AbstractClusterMessageHandler {

    private static final Log logger = LogFactory.getLog(ClusterMessageFailHandler.class);

    public ClusterMessageFailHandler(ClusterManagers managers) {
        super(managers);
    }

    @Override
    public boolean handle(ClusterNode sender, ClusterLink link, ClusterMessage hdr) {
        logger.debug("Fail packet received: node:" + (link.node == null ? "(nil)" : link.node.name));

        if (sender == null) return true;
        ClusterNode failing = managers.nodes.clusterLookupNode(hdr.data.fail.name);
        if (failing != null && !nodeIsMyself(failing.flags) && !nodeFailed(failing.flags)) {
            logger.info("FAIL message received from " + hdr.name + " fail " + hdr.data.fail.name);
            failing.flags |= CLUSTER_NODE_FAIL; failing.failTime = System.currentTimeMillis(); //fail time
            failing.flags &= ~CLUSTER_NODE_PFAIL; managers.notifyNodeFailed(valueOf(failing, server.myself));
        }
        return true;
    }
}

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

import com.moilioncircle.redis.cluster.watchdog.message.handler.ClusterMessageFailHandler;
import com.moilioncircle.redis.cluster.watchdog.message.handler.ClusterMessageFailoverAuthAckHandler;
import com.moilioncircle.redis.cluster.watchdog.message.handler.ClusterMessageFailoverAuthRequestHandler;
import com.moilioncircle.redis.cluster.watchdog.message.handler.ClusterMessageHandler;
import com.moilioncircle.redis.cluster.watchdog.message.handler.ClusterMessageMFStartHandler;
import com.moilioncircle.redis.cluster.watchdog.message.handler.ClusterMessageMeetHandler;
import com.moilioncircle.redis.cluster.watchdog.message.handler.ClusterMessagePingHandler;
import com.moilioncircle.redis.cluster.watchdog.message.handler.ClusterMessagePongHandler;
import com.moilioncircle.redis.cluster.watchdog.message.handler.ClusterMessagePublishHandler;
import com.moilioncircle.redis.cluster.watchdog.message.handler.ClusterMessageUpdateHandler;
import com.moilioncircle.redis.cluster.watchdog.util.collection.ByteMap;

import java.util.Map;

import static com.moilioncircle.redis.cluster.watchdog.ClusterConstants.CLUSTERMSG_TYPE_FAIL;
import static com.moilioncircle.redis.cluster.watchdog.ClusterConstants.CLUSTERMSG_TYPE_FAILOVER_AUTH_ACK;
import static com.moilioncircle.redis.cluster.watchdog.ClusterConstants.CLUSTERMSG_TYPE_FAILOVER_AUTH_REQUEST;
import static com.moilioncircle.redis.cluster.watchdog.ClusterConstants.CLUSTERMSG_TYPE_MEET;
import static com.moilioncircle.redis.cluster.watchdog.ClusterConstants.CLUSTERMSG_TYPE_MFSTART;
import static com.moilioncircle.redis.cluster.watchdog.ClusterConstants.CLUSTERMSG_TYPE_PING;
import static com.moilioncircle.redis.cluster.watchdog.ClusterConstants.CLUSTERMSG_TYPE_PONG;
import static com.moilioncircle.redis.cluster.watchdog.ClusterConstants.CLUSTERMSG_TYPE_PUBLISH;
import static com.moilioncircle.redis.cluster.watchdog.ClusterConstants.CLUSTERMSG_TYPE_UPDATE;

/**
 * @author Leon Chen
 * @since 1.0.0
 */
public class ClusterMessageHandlerManager {

    private Map<Byte, ClusterMessageHandler> handlers = new ByteMap<>();

    public ClusterMessageHandler get(int type) { return handlers.get((byte) type); }

    public void register(byte type, ClusterMessageHandler handler) { handlers.put(type, handler); }

    public ClusterMessageHandlerManager(ClusterManagers managers) {
        register((byte) CLUSTERMSG_TYPE_PING, new ClusterMessagePingHandler(managers));
        register((byte) CLUSTERMSG_TYPE_PONG, new ClusterMessagePongHandler(managers));
        register((byte) CLUSTERMSG_TYPE_MEET, new ClusterMessageMeetHandler(managers));
        register((byte) CLUSTERMSG_TYPE_FAIL, new ClusterMessageFailHandler(managers));
        register((byte) CLUSTERMSG_TYPE_UPDATE, new ClusterMessageUpdateHandler(managers));
        register((byte) CLUSTERMSG_TYPE_PUBLISH, new ClusterMessagePublishHandler(managers));
        register((byte) CLUSTERMSG_TYPE_MFSTART, new ClusterMessageMFStartHandler(managers));
        register((byte) CLUSTERMSG_TYPE_FAILOVER_AUTH_ACK, new ClusterMessageFailoverAuthAckHandler(managers));
        register((byte) CLUSTERMSG_TYPE_FAILOVER_AUTH_REQUEST, new ClusterMessageFailoverAuthRequestHandler(managers));
    }
}

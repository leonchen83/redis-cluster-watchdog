package com.moilioncircle.redis.cluster.watchdog.manager;

import com.moilioncircle.redis.cluster.watchdog.message.handler.*;
import com.moilioncircle.redis.cluster.watchdog.util.collection.ByteMap;

import java.util.Map;

import static com.moilioncircle.redis.cluster.watchdog.ClusterConstants.*;

/**
 * @author Leon Chen
 * @since 1.0.0
 */
public class ClusterMessageHandlerManager {

    private Map<Byte, ClusterMessageHandler> handlers = new ByteMap<>();

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

    public void register(byte type, ClusterMessageHandler handler) {
        handlers.put(type, handler);
    }

    public ClusterMessageHandler get(int type) {
        return handlers.get((byte) type);
    }
}

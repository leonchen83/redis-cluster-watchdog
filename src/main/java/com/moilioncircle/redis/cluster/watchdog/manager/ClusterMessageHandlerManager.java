package com.moilioncircle.redis.cluster.watchdog.manager;

import com.moilioncircle.redis.cluster.watchdog.message.handler.*;
import com.moilioncircle.redis.cluster.watchdog.util.collection.ByteMap;

import java.util.Map;

import static com.moilioncircle.redis.cluster.watchdog.ClusterConstants.*;

/**
 * Created by Baoyi Chen on 2017/7/13.
 */
public class ClusterMessageHandlerManager {

    private Map<Byte, ClusterMessageHandler> handlers = new ByteMap<>();

    public ClusterMessageHandlerManager(ClusterManagers gossip) {
        register((byte) CLUSTERMSG_TYPE_PING, new ClusterMessagePingHandler(gossip));
        register((byte) CLUSTERMSG_TYPE_PONG, new ClusterMessagePongHandler(gossip));
        register((byte) CLUSTERMSG_TYPE_MEET, new ClusterMessageMeetHandler(gossip));
        register((byte) CLUSTERMSG_TYPE_FAIL, new ClusterMessageFailHandler(gossip));
        register((byte) CLUSTERMSG_TYPE_UPDATE, new ClusterMessageUpdateHandler(gossip));
        register((byte) CLUSTERMSG_TYPE_PUBLISH, new ClusterMessagePublishHandler(gossip));
        register((byte) CLUSTERMSG_TYPE_MFSTART, new ClusterMessageMFStartHandler(gossip));
        register((byte) CLUSTERMSG_TYPE_FAILOVER_AUTH_ACK, new ClusterMessageFailoverAuthAckHandler(gossip));
        register((byte) CLUSTERMSG_TYPE_FAILOVER_AUTH_REQUEST, new ClusterMessageFailoverAuthRequestHandler(gossip));
    }

    public void register(byte type, ClusterMessageHandler handler) {
        handlers.put(type, handler);
    }

    public ClusterMessageHandler get(int type) {
        return handlers.get((byte) type);
    }
}

package com.moilioncircle.redis.cluster.watchdog.manager;

import com.moilioncircle.redis.cluster.watchdog.ClusterConstants;
import com.moilioncircle.redis.cluster.watchdog.message.handler.*;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by Baoyi Chen on 2017/7/13.
 */
public class ClusterMessageHandlerManager {

    private Map<Integer, ClusterMessageHandler> handlerMap = new HashMap<>();

    public ClusterMessageHandlerManager(ClusterManagers gossip) {
        register(ClusterConstants.CLUSTERMSG_TYPE_PING, new ClusterMessagePingHandler(gossip));
        register(ClusterConstants.CLUSTERMSG_TYPE_PONG, new ClusterMessagePongHandler(gossip));
        register(ClusterConstants.CLUSTERMSG_TYPE_MEET, new ClusterMessageMeetHandler(gossip));
        register(ClusterConstants.CLUSTERMSG_TYPE_FAIL, new ClusterMessageFailHandler(gossip));
        register(ClusterConstants.CLUSTERMSG_TYPE_UPDATE, new ClusterMessageUpdateHandler(gossip));
        register(ClusterConstants.CLUSTERMSG_TYPE_PUBLISH, new ClusterMessagePublishHandler(gossip));
        register(ClusterConstants.CLUSTERMSG_TYPE_MFSTART, new ClusterMessageMFStartHandler(gossip));
        register(ClusterConstants.CLUSTERMSG_TYPE_FAILOVER_AUTH_ACK, new ClusterMessageFailoverAuthAckHandler(gossip));
        register(ClusterConstants.CLUSTERMSG_TYPE_FAILOVER_AUTH_REQUEST, new ClusterMessageFailoverAuthRequestHandler(gossip));
    }

    public void register(int type, ClusterMessageHandler handler) {
        handlerMap.put(type, handler);
    }

    public ClusterMessageHandler get(int type) {
        return handlerMap.get(type);
    }
}

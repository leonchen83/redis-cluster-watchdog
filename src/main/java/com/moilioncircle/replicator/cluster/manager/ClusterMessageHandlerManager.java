package com.moilioncircle.replicator.cluster.manager;

import com.moilioncircle.replicator.cluster.message.handler.*;

import java.util.HashMap;
import java.util.Map;

import static com.moilioncircle.replicator.cluster.ClusterConstants.*;

/**
 * Created by Baoyi Chen on 2017/7/13.
 */
public class ClusterMessageHandlerManager {

    private Map<Integer, ClusterMessageHandler> handlerMap = new HashMap<>();

    public ClusterMessageHandlerManager(ClusterManagers gossip) {
        register(CLUSTERMSG_TYPE_PING, new ClusterMessagePingHandler(gossip));
        register(CLUSTERMSG_TYPE_PONG, new ClusterMessagePongHandler(gossip));
        register(CLUSTERMSG_TYPE_MEET, new ClusterMessageMeetHandler(gossip));
        register(CLUSTERMSG_TYPE_FAIL, new ClusterMessageFailHandler(gossip));
        register(CLUSTERMSG_TYPE_UPDATE, new ClusterMessageUpdateHandler(gossip));
        register(CLUSTERMSG_TYPE_PUBLISH, new ClusterMessagePublishHandler(gossip));
        register(CLUSTERMSG_TYPE_MFSTART, new ClusterMessageMFStartHandler(gossip));
        register(CLUSTERMSG_TYPE_FAILOVER_AUTH_ACK, new ClusterMessageFailoverAuthAckHandler(gossip));
        register(CLUSTERMSG_TYPE_FAILOVER_AUTH_REQUEST, new ClusterMessageFailoverAuthRequestHandler(gossip));
    }

    public void register(int type, ClusterMessageHandler handler) {
        handlerMap.put(type, handler);
    }

    public ClusterMessageHandler get(int type) {
        return handlerMap.get(type);
    }
}

package com.moilioncircle.replicator.cluster.gossip;

import com.moilioncircle.replicator.cluster.message.handler.*;

import java.util.HashMap;
import java.util.Map;

import static com.moilioncircle.replicator.cluster.ClusterConstants.*;

/**
 * Created by Baoyi Chen on 2017/7/13.
 */
public class ClusterMsgHandlerManager {

    private Map<Integer, ClusterMsgHandler> handlerMap = new HashMap<>();

    public ClusterMsgHandlerManager(ThinGossip gossip) {
        register(CLUSTERMSG_TYPE_PING, new ClusterMsgPingHandler(gossip));
        register(CLUSTERMSG_TYPE_PONG, new ClusterMsgPongHandler(gossip));
        register(CLUSTERMSG_TYPE_MEET, new ClusterMsgMeetHandler(gossip));
        register(CLUSTERMSG_TYPE_FAIL, new ClusterMsgFailHandler(gossip));
        register(CLUSTERMSG_TYPE_PUBLISH, new ClusterMsgPublishHandler(gossip));
        register(CLUSTERMSG_TYPE_FAILOVER_AUTH_REQUEST, new ClusterMsgFailoverAuthRequestHandler(gossip));
        register(CLUSTERMSG_TYPE_FAILOVER_AUTH_ACK, new ClusterMsgFailoverAuthAckHandler(gossip));
        register(CLUSTERMSG_TYPE_UPDATE, new ClusterMsgUpdateHandler(gossip));
        register(CLUSTERMSG_TYPE_MFSTART, new ClusterMsgMFStartHandler(gossip));
    }

    public void register(int type, ClusterMsgHandler handler) {
        handlerMap.put(type, handler);
    }

    public ClusterMsgHandler get(int type) {
        return handlerMap.get(type);
    }
}

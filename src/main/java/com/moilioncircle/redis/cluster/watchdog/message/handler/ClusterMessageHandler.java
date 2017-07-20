package com.moilioncircle.redis.cluster.watchdog.message.handler;

import com.moilioncircle.redis.cluster.watchdog.message.ClusterMessage;
import com.moilioncircle.redis.cluster.watchdog.state.ClusterLink;

/**
 * Created by Baoyi Chen on 2017/7/13.
 */
public interface ClusterMessageHandler {
    boolean handle(ClusterLink link, ClusterMessage hdr);
}

package com.moilioncircle.redis.cluster.watchdog.message.handler;

import com.moilioncircle.redis.cluster.watchdog.message.ClusterMessage;
import com.moilioncircle.redis.cluster.watchdog.state.ClusterLink;

/**
 * @author Leon Chen
 * @since 1.0.0
 */
public interface ClusterMessageHandler {
    boolean handle(ClusterLink link, ClusterMessage hdr);
}

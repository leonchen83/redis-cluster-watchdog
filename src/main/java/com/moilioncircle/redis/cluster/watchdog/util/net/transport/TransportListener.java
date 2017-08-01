package com.moilioncircle.redis.cluster.watchdog.util.net.transport;

/**
 * @author Leon Chen
 * @since 1.0.0
 */
public interface TransportListener<T> {

    default void onConnected(Transport<T> transport) {
    }

    default void onMessage(Transport<T> transport, T message) {
    }

    default void onException(Transport<T> transport, Throwable cause) {
    }

    default void onDisconnected(Transport<T> transport, Throwable cause) {
    }

}

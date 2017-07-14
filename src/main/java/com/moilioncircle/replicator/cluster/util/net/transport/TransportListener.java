package com.moilioncircle.replicator.cluster.util.net.transport;

/**
 * Created by Baoyi Chen on 2017/7/7.
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

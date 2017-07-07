package com.moilioncircle.replicator.cluster.util.net.transport;

/**
 * Created by Baoyi Chen on 2017/7/7.
 */
public interface TransportListener<T> {
    void onConnected(Transport<T> transport);

    void onMessage(Transport<T> transport, T message);

    void onException(Transport<T> transport, Throwable cause);

    void onDisconnected(Transport<T> transport, Throwable cause);
}

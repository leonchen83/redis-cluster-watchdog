package com.moilioncircle.redis.cluster.watchdog.util.net.transport;

/**
 * @author Leon Chen
 * @since 1.0.0
 */
public interface TransportListener<T> {

    void onConnected(Transport<T> transport);

    void onMessage(Transport<T> transport, T message);

    void onException(Transport<T> transport, Throwable cause);

    void onDisconnected(Transport<T> transport, Throwable cause);

    abstract class Adaptor<T> implements TransportListener<T> {

        public void onConnected(Transport<T> transport) {}

        public void onMessage(Transport<T> transport, T message) {}

        public void onException(Transport<T> transport, Throwable cause) {}

        public void onDisconnected(Transport<T> transport, Throwable cause) {}

    }
}

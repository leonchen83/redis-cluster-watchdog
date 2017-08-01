package com.moilioncircle.redis.cluster.watchdog.util.net.transport;

import com.moilioncircle.redis.cluster.watchdog.util.concurrent.future.CompletableFuture;
import com.moilioncircle.redis.cluster.watchdog.util.net.ConnectionStatus;

import java.net.SocketAddress;

/**
 * @author Leon Chen
 * @since 1.0.0
 */
public interface Transport<T> {

    long getId();

    ConnectionStatus getStatus();

    SocketAddress getLocalAddress();

    SocketAddress getRemoteAddress();

    CompletableFuture<Void> disconnect(Throwable cause);

    CompletableFuture<Void> write(T message, boolean flush);

    TransportListener<T> setTransportListener(TransportListener<T> listener);
}

package com.moilioncircle.redis.cluster.watchdog.util.net.transport;

import com.moilioncircle.redis.cluster.watchdog.util.concurrent.future.CompletableFuture;
import com.moilioncircle.redis.cluster.watchdog.util.net.ConnectionStatus;

import java.net.SocketAddress;

/**
 * Created by Baoyi Chen on 2017/7/6.
 */
public interface Transport<T> {

    long getId();

    ConnectionStatus getStatus();

    SocketAddress getLocalAddress();

    SocketAddress getRemoteAddress();

    CompletableFuture<Void> disconnect(Throwable cause);

    CompletableFuture<Void> write(T message, boolean flush);
}

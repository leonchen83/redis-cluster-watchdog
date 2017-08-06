package com.moilioncircle.redis.cluster.watchdog.util.net.session;

import com.moilioncircle.redis.cluster.watchdog.util.concurrent.future.CompletableFuture;
import com.moilioncircle.redis.cluster.watchdog.util.net.ConnectionStatus;

/**
 * @author Leon Chen
 * @since 1.0.0
 */
public interface Session<T> {

    long getId();

    ConnectionStatus getStatus();

    String getLocalAddress(String value);

    String getRemoteAddress(String value);

    CompletableFuture<Void> send(T message);

    CompletableFuture<Void> disconnect(Throwable cause);
}

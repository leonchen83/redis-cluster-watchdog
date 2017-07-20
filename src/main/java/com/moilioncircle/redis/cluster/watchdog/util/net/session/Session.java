package com.moilioncircle.redis.cluster.watchdog.util.net.session;

import com.moilioncircle.redis.cluster.watchdog.util.concurrent.future.CompletableFuture;
import com.moilioncircle.redis.cluster.watchdog.util.net.ConnectionStatus;

/**
 * Created by Baoyi Chen on 2017/7/7.
 */
public interface Session<T> {

    String getLocalAddress(String value);

    String getRemoteAddress(String value);

    long getId();

    ConnectionStatus getStatus();

    CompletableFuture<Void> send(T message);

    CompletableFuture<Void> disconnect(Throwable cause);
}

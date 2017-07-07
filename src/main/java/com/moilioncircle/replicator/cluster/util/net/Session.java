package com.moilioncircle.replicator.cluster.util.net;

import com.moilioncircle.replicator.cluster.util.concurrent.future.CompletableFuture;

/**
 * Created by Baoyi Chen on 2017/7/7.
 */
public interface Session<T> {

    long getId();

    Status getStatus();

    CompletableFuture<Void> send(T message);

    CompletableFuture<Void> disconnect(Throwable cause);
}

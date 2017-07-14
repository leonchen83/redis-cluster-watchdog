package com.moilioncircle.replicator.cluster.util.net.session;

import com.moilioncircle.replicator.cluster.util.concurrent.future.CompletableFuture;
import com.moilioncircle.replicator.cluster.util.net.ConnectionStatus;

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

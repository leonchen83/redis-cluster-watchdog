package com.moilioncircle.replicator.cluster.util.net.transport;

import com.moilioncircle.replicator.cluster.util.concurrent.future.CompletableFuture;
import com.moilioncircle.replicator.cluster.util.net.Status;

import java.net.SocketAddress;

/**
 * Created by Baoyi Chen on 2017/7/6.
 */
public interface Transport<T> {

    long getId();

    Status getStatus();

    SocketAddress getLocalAddress();

    SocketAddress getRemoteAddress();

    CompletableFuture<Void> disconnect(Throwable cause);

    CompletableFuture<Void> write(T message, boolean flush);

    TransportListener<T> setTransportListener(TransportListener<T> listener);
}

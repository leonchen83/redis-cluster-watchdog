package com.moilioncircle.replicator.cluster.util.net;

import com.moilioncircle.replicator.cluster.util.concurrent.future.CompletableFuture;
import com.moilioncircle.replicator.cluster.util.concurrent.future.ListenableFuture;
import com.moilioncircle.replicator.cluster.util.net.transport.Transport;

/**
 * Created by Baoyi Chen on 2017/7/7.
 */
public class SessionImpl<T> implements Session<T> {
    private final Transport<T> transport;

    public SessionImpl(Transport<T> transport) {
        this.transport = transport;
    }

    @Override
    public long getId() {
        return transport.getId();
    }

    @Override
    public Status getStatus() {
        return this.transport.getStatus();
    }

    @Override
    public CompletableFuture<Void> send(T message) {
        if (transport.getStatus() == Status.CONNECTED) {
            return transport.write(message, true);
        } else {
            CompletableFuture<Void> r = new ListenableFuture<>();
            r.failure(new ServiceTransportException());
            return r;
        }
    }

    @Override
    public CompletableFuture<Void> disconnect(Throwable cause) {
        return transport.disconnect(cause);
    }

    @Override
    public String toString() {
        return transport.toString();
    }

}

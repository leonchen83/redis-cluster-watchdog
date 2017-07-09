package com.moilioncircle.replicator.cluster.util.net.session;

import com.moilioncircle.replicator.cluster.util.concurrent.future.CompletableFuture;
import com.moilioncircle.replicator.cluster.util.concurrent.future.ListenableFuture;
import com.moilioncircle.replicator.cluster.util.net.ConnectionStatus;
import com.moilioncircle.replicator.cluster.util.net.exceptions.TransportException;
import com.moilioncircle.replicator.cluster.util.net.transport.Transport;

/**
 * Created by Baoyi Chen on 2017/7/7.
 */
public class SessionImpl<T> implements Session<T> {

    protected final Transport<T> transport;

    public SessionImpl(Transport<T> transport) {
        this.transport = transport;
    }

    @Override
    public String getLocalAddress() {
        return transport.getLocalAddress().toString();
    }

    @Override
    public String getRemoteAddress() {
        return transport.getRemoteAddress().toString();
    }

    @Override
    public long getId() {
        return transport.getId();
    }

    @Override
    public ConnectionStatus getStatus() {
        return this.transport.getStatus();
    }

    @Override
    public CompletableFuture<Void> send(T message) {
        if (transport.getStatus() == ConnectionStatus.CONNECTED) {
            return transport.write(message, true);
        } else {
            CompletableFuture<Void> r = new ListenableFuture<>();
            r.failure(new TransportException("[cluster] connection disconnected: " + toString()));
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

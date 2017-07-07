package com.moilioncircle.replicator.cluster.util.net.initiator;

import com.moilioncircle.replicator.cluster.util.marshaller.Marshaller;
import com.moilioncircle.replicator.cluster.util.net.transport.Transport;
import com.moilioncircle.replicator.cluster.util.net.transport.TransportListener;
import io.netty.channel.ChannelHandler;

import java.util.function.Supplier;

/**
 * Created by Baoyi Chen on 2017/7/7.
 */
public abstract class AbstractNioInitiator<T> implements Initiator<T> {
    protected Supplier<ChannelHandler> encoder;
    protected Supplier<ChannelHandler> decoder;
    protected volatile Marshaller<T> marshaller;
    protected volatile TransportListener<T> listener;

    @Override
    public void onMessage(Transport<T> transport, T message) {
        TransportListener<T> listener = this.listener;
        if (listener != null) listener.onMessage(transport, message);
    }

    @Override
    public void onException(Transport<T> transport, Throwable throwable) {
        TransportListener<T> listener = this.listener;
        if (listener != null) listener.onException(transport, throwable);
    }

    @Override
    public void onConnected(Transport<T> transport) {
        TransportListener<T> listener = this.listener;
        if (listener != null) listener.onConnected(transport);
    }

    @Override
    public void onDisconnected(Transport<T> transport, Throwable cause) {
        TransportListener<T> listener = this.listener;
        if (listener != null) listener.onDisconnected(transport, cause);
    }

    @Override
    public void setMarshaller(Marshaller<T> marshaller) {
        this.marshaller = marshaller;
    }

    @Override
    public TransportListener<T> setTransportListener(TransportListener<T> listener) {
        TransportListener<T> oldListener = this.listener;
        this.listener = listener;
        return oldListener;
    }

    public Supplier<ChannelHandler> getEncoder() {
        return encoder;
    }

    public void setEncoder(Supplier<ChannelHandler> encoder) {
        this.encoder = encoder;
    }

    public Supplier<ChannelHandler> getDecoder() {
        return decoder;
    }

    public void setDecoder(Supplier<ChannelHandler> decoder) {
        this.decoder = decoder;
    }
}

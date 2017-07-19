package com.moilioncircle.replicator.cluster.util.net.transport;

import com.moilioncircle.replicator.cluster.util.concurrent.future.CompletableFuture;
import com.moilioncircle.replicator.cluster.util.concurrent.future.ListenableChannelFuture;
import com.moilioncircle.replicator.cluster.util.net.AbstractNioBootstrap;
import com.moilioncircle.replicator.cluster.util.net.ConnectionStatus;
import com.moilioncircle.replicator.cluster.util.net.exceptions.OverloadException;
import com.moilioncircle.replicator.cluster.util.net.exceptions.TransportException;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

import java.io.IOException;
import java.net.SocketAddress;
import java.util.concurrent.atomic.AtomicInteger;

import static com.moilioncircle.replicator.cluster.util.net.ConnectionStatus.CONNECTED;
import static com.moilioncircle.replicator.cluster.util.net.ConnectionStatus.DISCONNECTED;

/**
 * Created by Baoyi Chen on 2017/7/7.
 */
@SuppressWarnings("unchecked")
public class NioTransport<T> extends SimpleChannelInboundHandler<T> implements Transport<T> {

    private long id;
    private volatile Channel channel;
    private volatile ChannelHandlerContext context;
    private volatile AbstractNioBootstrap<T> listener;
    private static AtomicInteger acc = new AtomicInteger();

    public NioTransport(AbstractNioBootstrap<T> listener) {
        super((Class<T>) Object.class);
        this.listener = listener;
        this.id = acc.incrementAndGet();
    }

    public boolean acceptInboundMessage(Object msg) throws Exception {
        return true;
    }

    public void setChannel(Channel channel) {
        this.channel = channel;
    }

    @Override
    public long getId() {
        return id;
    }

    @Override
    public SocketAddress getRemoteAddress() {
        if (listener.isServer()) {
            if (this.context == null) return null;
            return this.context.channel().remoteAddress();
        } else {
            if (this.channel == null) return null;
            return this.channel.remoteAddress();
        }
    }

    @Override
    public SocketAddress getLocalAddress() {
        if (listener.isServer()) {
            if (this.context == null) return null;
            return this.context.channel().localAddress();
        } else {
            if (this.channel == null) return null;
            return this.channel.localAddress();
        }
    }

    @Override
    public ConnectionStatus getStatus() {
        if (listener.isServer()) {
            if (this.context == null) return DISCONNECTED;
            return this.context.channel().isActive() ? CONNECTED : DISCONNECTED;
        } else {
            if (this.channel == null) return DISCONNECTED;
            return this.channel.isActive() ? CONNECTED : DISCONNECTED;
        }
    }

    @Override
    public CompletableFuture<Void> write(T message, boolean flush) {
        if (listener.isServer()) {
            if (!flush) {
                return new ListenableChannelFuture<>(context.write(message));
            } else {
                return new ListenableChannelFuture<>(context.writeAndFlush(message));
            }
        } else {
            if (!flush) {
                return new ListenableChannelFuture<>(channel.write(message));
            } else {
                return new ListenableChannelFuture<>(channel.writeAndFlush(message));
            }
        }
    }

    @Override
    public CompletableFuture<Void> disconnect(Throwable cause) {
        return new ListenableChannelFuture<>(this.context.close());
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        super.channelActive(this.context = ctx);
        TransportListener<T> listener = this.listener;
        if (listener != null) listener.onConnected(this);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        super.channelInactive(ctx);
        TransportListener<T> listener = this.listener;
        if (listener != null) listener.onDisconnected(this, null);
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, T message) throws Exception {
        TransportListener<T> listener = this.listener;
        if (listener != null) listener.onMessage(this, message);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        if (cause instanceof IOException) cause = new TransportException(toString(), cause);
        TransportListener<T> listener = this.listener;
        if (listener != null) listener.onException(this, cause);
    }

    @Override
    public final void channelWritabilityChanged(final ChannelHandlerContext ctx) throws Exception {
        if (ctx.channel().isWritable()) return;
        TransportListener<T> listener = this.listener;
        if (listener != null) listener.onException(this, new OverloadException("overload"));
    }

    @Override
    public String toString() {
        return new StringBuilder()
                .append("[").append("id=").append(id)
                .append(",la=").append(getLocalAddress())
                .append(",ra=").append(getRemoteAddress()).append("]").toString();
    }

}

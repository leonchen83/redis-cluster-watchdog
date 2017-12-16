/*
 * Copyright 2016-2017 Leon Chen
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.moilioncircle.redis.cluster.watchdog.util.net.transport;

import com.moilioncircle.redis.cluster.watchdog.util.net.AbstractNioBootstrap;
import com.moilioncircle.redis.cluster.watchdog.util.net.exceptions.OverloadException;
import com.moilioncircle.redis.cluster.watchdog.util.net.exceptions.TransportException;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Leon Chen
 * @since 1.0.0
 */
@SuppressWarnings("unchecked")
public abstract class AbstractTransport<T> extends SimpleChannelInboundHandler<T> implements Transport<T> {

    private long id;
    protected volatile TransportListener<T> listener;
    private static AtomicInteger acc = new AtomicInteger();

    protected AbstractTransport(AbstractNioBootstrap<T> listener) {
        super((Class<T>) Object.class);
        this.listener = listener;
        this.id = acc.incrementAndGet();
    }

    @Override
    public boolean acceptInboundMessage(Object msg) throws Exception {
        return true;
    }

    @Override
    public long getId() {
        return id;
    }

    @Override
    public synchronized TransportListener<T> setTransportListener(TransportListener<T> listener) {
        TransportListener<T> r = this.listener; this.listener = listener; return r;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        TransportListener<T> listener = this.listener;
        if (listener != null) listener.onConnected(this);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
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
        return "[" + "id=" + id +
                ",la=" + getLocalAddress() +
                ",ra=" + getRemoteAddress() + "]";
    }
}

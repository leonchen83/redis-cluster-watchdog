/*
 * Copyright 2016-2018 Leon Chen
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

import com.moilioncircle.redis.cluster.watchdog.util.concurrent.future.CompletableFuture;
import com.moilioncircle.redis.cluster.watchdog.util.concurrent.future.ListenableChannelFuture;
import com.moilioncircle.redis.cluster.watchdog.util.net.AbstractNioBootstrap;
import com.moilioncircle.redis.cluster.watchdog.util.net.ConnectionStatus;
import io.netty.channel.ChannelHandlerContext;

import java.net.SocketAddress;

import static com.moilioncircle.redis.cluster.watchdog.util.net.ConnectionStatus.CONNECTED;
import static com.moilioncircle.redis.cluster.watchdog.util.net.ConnectionStatus.DISCONNECTED;

/**
 * @author Leon Chen
 * @since 1.0.0
 */
public class NioAcceptorTransport<T> extends AbstractTransport<T> {

    private volatile ChannelHandlerContext context;

    public NioAcceptorTransport(AbstractNioBootstrap<T> listener) {
        super(listener);
    }

    @Override
    public ConnectionStatus getStatus() {
        if (this.context == null) return DISCONNECTED;
        return this.context.channel().isActive() ? CONNECTED : DISCONNECTED;
    }

    @Override
    public SocketAddress getLocalAddress() {
        if (this.context == null) return null;
        return this.context.channel().localAddress();
    }

    @Override
    public SocketAddress getRemoteAddress() {
        if (this.context == null) return null;
        return this.context.channel().remoteAddress();
    }

    @Override
    public CompletableFuture<Void> disconnect(Throwable cause) {
        return new ListenableChannelFuture<>(this.context.close());
    }

    @Override
    public <V> CompletableFuture<Void> write(V message, boolean flush) {
        if (!flush) { return new ListenableChannelFuture<>(context.write(message)); }
        else { return new ListenableChannelFuture<>(context.writeAndFlush(message)); }
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        super.channelActive(this.context = ctx);
    }
}

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

import com.moilioncircle.redis.cluster.watchdog.util.concurrent.future.CompletableFuture;
import com.moilioncircle.redis.cluster.watchdog.util.concurrent.future.ListenableChannelFuture;
import com.moilioncircle.redis.cluster.watchdog.util.net.AbstractNioBootstrap;
import com.moilioncircle.redis.cluster.watchdog.util.net.ConnectionStatus;
import io.netty.channel.Channel;

import java.net.SocketAddress;

/**
 * @author Leon Chen
 * @since 1.0.0
 */
public class NioInitiatorTransport<T> extends AbstractTransport<T> {

    private volatile Channel channel;

    public NioInitiatorTransport(AbstractNioBootstrap<T> listener) {
        super(listener);
    }

    @Override
    public ConnectionStatus getStatus() {
        if (this.channel == null) return ConnectionStatus.DISCONNECTED;
        return this.channel.isActive() ? ConnectionStatus.CONNECTED : ConnectionStatus.DISCONNECTED;
    }

    @Override
    public SocketAddress getLocalAddress() {
        if (this.channel == null) return null;
        return this.channel.localAddress();
    }

    @Override
    public SocketAddress getRemoteAddress() {
        if (this.channel == null) return null;
        return this.channel.remoteAddress();
    }

    @Override
    public CompletableFuture<Void> disconnect(Throwable cause) {
        return new ListenableChannelFuture<>(channel.close());
    }

    @Override
    public CompletableFuture<Void> write(T message, boolean flush) {
        if (!flush) {
            return new ListenableChannelFuture<>(channel.write(message));
        } else {
            return new ListenableChannelFuture<>(channel.writeAndFlush(message));
        }
    }

    public void setChannel(Channel channel) {
        this.channel = channel;
    }
}

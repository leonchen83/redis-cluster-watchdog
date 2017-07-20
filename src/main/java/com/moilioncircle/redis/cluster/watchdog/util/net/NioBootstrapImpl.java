/*
 * Copyright 2016 leon chen
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

package com.moilioncircle.redis.cluster.watchdog.util.net;

import com.moilioncircle.redis.cluster.watchdog.util.concurrent.future.CompletableFuture;
import com.moilioncircle.redis.cluster.watchdog.util.net.transport.Transport;
import com.moilioncircle.redis.cluster.watchdog.util.net.transport.TransportListener;
import io.netty.channel.ChannelHandler;

import java.util.function.Supplier;

/**
 * @author Leon Chen
 * @since 2.1.0
 */
public class NioBootstrapImpl<T> implements NioBootstrap<T> {

    private NioBootstrap<T> wrapper;

    public NioBootstrapImpl(boolean server, NioBootstrapConfiguration configuration) {
        if (server) wrapper = new NioAcceptor<>(configuration);
        else wrapper = new NioInitiator<>(configuration);
    }

    @Override
    public void onConnected(Transport<T> transport) {
        wrapper.onConnected(transport);
    }

    @Override
    public void onMessage(Transport<T> transport, T message) {
        wrapper.onMessage(transport, message);
    }

    @Override
    public void onException(Transport<T> transport, Throwable cause) {
        wrapper.onException(transport, cause);
    }

    @Override
    public void onDisconnected(Transport<T> transport, Throwable cause) {
        wrapper.onDisconnected(transport, cause);
    }

    @Override
    public void setup() {
        wrapper.setup();
    }

    @Override
    public boolean isServer() {
        return wrapper.isServer();
    }

    @Override
    public Transport<T> getTransport() {
        return wrapper.getTransport();
    }

    @Override
    public CompletableFuture<?> shutdown() {
        return wrapper.shutdown();
    }

    @Override
    public void setEncoder(Supplier<ChannelHandler> encoder) {
        wrapper.setEncoder(encoder);
    }

    @Override
    public void setDecoder(Supplier<ChannelHandler> decoder) {
        wrapper.setDecoder(decoder);
    }

    @Override
    public CompletableFuture<Void> connect(String host, int port) {
        return wrapper.connect(host, port);
    }

    @Override
    public TransportListener<T> setTransportListener(TransportListener<T> listener) {
        return wrapper.setTransportListener(listener);
    }
}

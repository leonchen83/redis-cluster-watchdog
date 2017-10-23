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

package com.moilioncircle.redis.cluster.watchdog.util.net;

import com.moilioncircle.redis.cluster.watchdog.util.net.transport.Transport;
import com.moilioncircle.redis.cluster.watchdog.util.net.transport.TransportListener;
import io.netty.channel.ChannelHandler;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.function.Supplier;

/**
 * @author Leon Chen
 * @since 1.0.0
 */
public abstract class AbstractNioBootstrap<T> implements NioBootstrap<T> {
    protected static final Log logger = LogFactory.getLog(AbstractNioBootstrap.class);

    protected Supplier<ChannelHandler> encoder;
    protected Supplier<ChannelHandler> decoder;

    protected volatile TransportListener<T> listener;
    protected final NetworkConfiguration configuration;

    protected AbstractNioBootstrap(NetworkConfiguration configuration) {
        this.configuration = configuration;
    }

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
    public TransportListener<T> setTransportListener(TransportListener<T> listener) {
        TransportListener<T> r = this.listener; this.listener = listener; return r;
    }

    public Supplier<ChannelHandler> getEncoder() {
        return encoder;
    }

    @Override
    public void setEncoder(Supplier<ChannelHandler> encoder) {
        this.encoder = encoder;
    }

    public Supplier<ChannelHandler> getDecoder() {
        return decoder;
    }

    @Override
    public void setDecoder(Supplier<ChannelHandler> decoder) {
        this.decoder = decoder;
    }
}

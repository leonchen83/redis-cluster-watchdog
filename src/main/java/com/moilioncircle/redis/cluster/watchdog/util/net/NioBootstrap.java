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
 * @since 1.0.0
 */
public interface NioBootstrap<T> extends TransportListener<T> {

    void setup();

    boolean isServer();

    Transport<T> getTransport();

    CompletableFuture<?> shutdown();

    void setEncoder(Supplier<ChannelHandler> encoder);

    void setDecoder(Supplier<ChannelHandler> decoder);

    CompletableFuture<Void> connect(String host, int port);

    TransportListener<T> setTransportListener(TransportListener<T> listener);
}

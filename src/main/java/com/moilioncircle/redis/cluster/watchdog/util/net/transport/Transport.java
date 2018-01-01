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
import com.moilioncircle.redis.cluster.watchdog.util.net.ConnectionStatus;

import java.net.SocketAddress;

/**
 * @author Leon Chen
 * @since 1.0.0
 */
public interface Transport<T> {

    long getId();

    ConnectionStatus getStatus();

    SocketAddress getLocalAddress();

    SocketAddress getRemoteAddress();

    CompletableFuture<Void> disconnect(Throwable cause);

    <V> CompletableFuture<Void> write(V message, boolean flush);

    TransportListener<T> setTransportListener(TransportListener<T> listener);
}

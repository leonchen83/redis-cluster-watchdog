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

package com.moilioncircle.redis.cluster.watchdog.util.net.session;

import com.moilioncircle.redis.cluster.watchdog.util.concurrent.future.CompletableFuture;
import com.moilioncircle.redis.cluster.watchdog.util.concurrent.future.ListenableFuture;
import com.moilioncircle.redis.cluster.watchdog.util.net.ConnectionStatus;
import com.moilioncircle.redis.cluster.watchdog.util.net.exceptions.TransportException;
import com.moilioncircle.redis.cluster.watchdog.util.net.transport.Transport;

import java.net.InetSocketAddress;

/**
 * @author Leon Chen
 * @since 1.0.0
 */
public class DefaultSession<T> implements Session<T> {
    
    protected final Transport<T> transport;
    
    public DefaultSession(Transport<T> transport) {
        this.transport = transport;
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
    public String getLocalAddress(String value) {
        if (value != null) return value;
        return ((InetSocketAddress) transport.getLocalAddress()).getAddress().getHostAddress();
    }
    
    @Override
    public String getRemoteAddress(String value) {
        if (value != null) return value;
        return ((InetSocketAddress) transport.getRemoteAddress()).getAddress().getHostAddress();
    }
    
    @Override
    public CompletableFuture<Void> send(T message) {
        if (transport.getStatus() == ConnectionStatus.CONNECTED) {
            return transport.write(message, true);
        } else {
            CompletableFuture<Void> r = new ListenableFuture<>();
            r.failure(new TransportException("connection disconnected: " + toString()));
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

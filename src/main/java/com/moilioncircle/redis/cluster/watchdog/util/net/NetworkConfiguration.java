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

import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * @author Leon Chen
 * @since 1.0.0
 */
public class NetworkConfiguration {

    private NetworkConfiguration() {
    }

    public static NetworkConfiguration defaultSetting() {
        return new NetworkConfiguration();
    }

    protected int soLinger = 0;
    protected int soTimeout = 0;
    protected int soBacklog = 1024;
    protected int soSendBufferSize = 0;
    protected int soRecvBufferSize = 0;
    protected boolean tcpNoDelay = true;
    protected boolean soKeepAlive = true;
    protected boolean soReuseAddr = true;
    protected boolean autoReconnect = false;
    protected volatile long connectTimeout = SECONDS.toMillis(5);
    protected volatile long reconnectInterval = SECONDS.toMillis(5);

    /**
     *
     */
    public int getSoLinger() {
        return soLinger;
    }

    public int getSoTimeout() {
        return soTimeout;
    }

    public int getSoBacklog() {
        return soBacklog;
    }

    public boolean isTcpNoDelay() {
        return tcpNoDelay;
    }

    public boolean isSoKeepAlive() {
        return soKeepAlive;
    }

    public boolean isSoReuseAddr() {
        return soReuseAddr;
    }

    public long getConnectTimeout() {
        return connectTimeout;
    }

    public boolean isAutoReconnect() {
        return autoReconnect;
    }

    public int getSoSendBufferSize() {
        return soSendBufferSize;
    }

    public int getSoRecvBufferSize() {
        return soRecvBufferSize;
    }

    public long getReconnectInterval() {
        return reconnectInterval;
    }

    /**
     *
     */
    public NetworkConfiguration setSoLinger(int soLinger) {
        this.soLinger = soLinger;
        return this;
    }

    public NetworkConfiguration setSoTimeout(int soTimeout) {
        this.soTimeout = soTimeout;
        return this;
    }

    public NetworkConfiguration setSoBacklog(int soBacklog) {
        this.soBacklog = soBacklog;
        return this;
    }

    public NetworkConfiguration setTcpNoDelay(boolean tcpNoDelay) {
        this.tcpNoDelay = tcpNoDelay;
        return this;
    }

    public NetworkConfiguration setSoKeepAlive(boolean soKeepAlive) {
        this.soKeepAlive = soKeepAlive;
        return this;
    }

    public NetworkConfiguration setSoReuseAddr(boolean soReuseAddr) {
        this.soReuseAddr = soReuseAddr;
        return this;
    }

    public NetworkConfiguration setConnectTimeout(long connectTimeout) {
        this.connectTimeout = connectTimeout;
        return this;
    }

    public NetworkConfiguration setAutoReconnect(boolean autoReconnect) {
        this.autoReconnect = autoReconnect;
        return this;
    }

    public NetworkConfiguration setSoSendBufferSize(int soSendBufferSize) {
        this.soSendBufferSize = soSendBufferSize;
        return this;
    }

    public NetworkConfiguration setSoRecvBufferSize(int soRecvBufferSize) {
        this.soRecvBufferSize = soRecvBufferSize;
        return this;
    }

    public NetworkConfiguration setReconnectInterval(long reconnectInterval) {
        this.reconnectInterval = reconnectInterval;
        return this;
    }
}

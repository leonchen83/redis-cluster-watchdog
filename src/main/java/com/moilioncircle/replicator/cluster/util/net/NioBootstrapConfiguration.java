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

package com.moilioncircle.replicator.cluster.util.net;

import java.util.concurrent.TimeUnit;

/**
 * @author Leon Chen
 * @since 2.1.0
 */
public class NioBootstrapConfiguration {

    protected int soLinger = 0;

    protected int soTimeout = 0;

    protected int soBacklog = 1024;

    protected int soSendBufferSize = 0;

    protected int soRecvBufferSize = 0;

    protected boolean tcpNoDelay = true;

    protected boolean soKeepAlive = true;

    protected boolean soReuseAddr = true;

    protected boolean autoReconnect = false;

    protected volatile long connectTimeout = TimeUnit.SECONDS.toMillis(10);

    protected volatile long reconnectInterval = TimeUnit.SECONDS.toMillis(15);

    public int getSoLinger() {
        return soLinger;
    }

    public void setSoLinger(int soLinger) {
        this.soLinger = soLinger;
    }

    public int getSoTimeout() {
        return soTimeout;
    }

    public void setSoTimeout(int soTimeout) {
        this.soTimeout = soTimeout;
    }

    public int getSoBacklog() {
        return soBacklog;
    }

    public void setSoBacklog(int soBacklog) {
        this.soBacklog = soBacklog;
    }

    public int getSoSendBufferSize() {
        return soSendBufferSize;
    }

    public void setSoSendBufferSize(int soSendBufferSize) {
        this.soSendBufferSize = soSendBufferSize;
    }

    public int getSoRecvBufferSize() {
        return soRecvBufferSize;
    }

    public void setSoRecvBufferSize(int soRecvBufferSize) {
        this.soRecvBufferSize = soRecvBufferSize;
    }

    public boolean isTcpNoDelay() {
        return tcpNoDelay;
    }

    public void setTcpNoDelay(boolean tcpNoDelay) {
        this.tcpNoDelay = tcpNoDelay;
    }

    public boolean isSoKeepAlive() {
        return soKeepAlive;
    }

    public void setSoKeepAlive(boolean soKeepAlive) {
        this.soKeepAlive = soKeepAlive;
    }

    public boolean isSoReuseAddr() {
        return soReuseAddr;
    }

    public void setSoReuseAddr(boolean soReuseAddr) {
        this.soReuseAddr = soReuseAddr;
    }

    public boolean isAutoReconnect() {
        return autoReconnect;
    }

    public void setAutoReconnect(boolean autoReconnect) {
        this.autoReconnect = autoReconnect;
    }

    public long getConnectTimeout() {
        return connectTimeout;
    }

    public void setConnectTimeout(long connectTimeout) {
        this.connectTimeout = connectTimeout;
    }

    public long getReconnectInterval() {
        return reconnectInterval;
    }

    public void setReconnectInterval(long reconnectInterval) {
        this.reconnectInterval = reconnectInterval;
    }
}

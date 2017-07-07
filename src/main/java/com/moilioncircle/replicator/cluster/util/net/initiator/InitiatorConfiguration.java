package com.moilioncircle.replicator.cluster.util.net.initiator;

import java.util.concurrent.TimeUnit;

/**
 * Created by Baoyi Chen on 2017/7/7.
 */
public class InitiatorConfiguration {
    protected int soSendBufferSize = 0;

    protected int soRecvBufferSize = 0;

    protected boolean tcpNoDelay = true;

    protected boolean soKeepAlive = true;

    protected volatile boolean autoReconnect = false;

    protected volatile long connectTimeout = TimeUnit.SECONDS.toMillis(10);

    protected volatile long reconnectInterval = TimeUnit.SECONDS.toMillis(15);

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

package com.moilioncircle.replicator.cluster.util.net.acceptor;

import io.netty.util.internal.PlatformDependent;

/**
 * Created by Baoyi Chen on 2017/7/7.
 */
public class AcceptorConfiguration {
    protected int soLinger;
    protected int soTimeout;
    protected int soBacklog = 1024;
    protected int soSendBufferSize;
    protected int soRecvBufferSize;
    protected boolean tcpNoDelay = true;
    protected boolean soKeepAlive = true;
    protected boolean soReuseAddr = true;
    protected int soSendBufferLowWaterMark;
    protected int soSendBufferHighWaterMark;
    protected int eventLoopThreads;
    protected boolean poolingEnabled;
    protected boolean preferDirect = PlatformDependent.directBufferPreferred();

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

    public int getSoSendBufferLowWaterMark() {
        return soSendBufferLowWaterMark;
    }

    public void setSoSendBufferLowWaterMark(int soSendBufferLowWaterMark) {
        this.soSendBufferLowWaterMark = soSendBufferLowWaterMark;
    }

    public int getSoSendBufferHighWaterMark() {
        return soSendBufferHighWaterMark;
    }

    public void setSoSendBufferHighWaterMark(int soSendBufferHighWaterMark) {
        this.soSendBufferHighWaterMark = soSendBufferHighWaterMark;
    }

    public int getEventLoopThreads() {
        return eventLoopThreads;
    }

    public void setEventLoopThreads(int eventLoopThreads) {
        this.eventLoopThreads = eventLoopThreads;
    }

    public boolean isPoolingEnabled() {
        return poolingEnabled;
    }

    public void setPoolingEnabled(boolean poolingEnabled) {
        this.poolingEnabled = poolingEnabled;
    }

    public boolean isPreferDirect() {
        return preferDirect;
    }

    public void setPreferDirect(boolean preferDirect) {
        this.preferDirect = preferDirect;
    }
}

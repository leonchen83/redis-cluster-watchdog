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

import com.moilioncircle.replicator.cluster.util.concurrent.future.CompletableFuture;
import com.moilioncircle.replicator.cluster.util.concurrent.future.ListenableChannelFuture;
import com.moilioncircle.replicator.cluster.util.net.transport.NioTransport;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.util.concurrent.DefaultThreadFactory;

import static io.netty.channel.ChannelOption.WRITE_BUFFER_WATER_MARK;

/**
 * @author Leon Chen
 * @since 2.1.0
 */
public class AbstractNioAcceptor<T> extends AbstractNioBootstrap<T> {
    protected EventLoopGroup eventLoop;
    protected ServerBootstrap bootstrap;

    protected AbstractNioAcceptor(Class<T> messageType, NioBootstrapConfiguration configuration) {
        super(messageType, configuration);
    }

    @Override
    public void setup() {
        this.eventLoop = new NioEventLoopGroup(configuration.getEventLoopThreads(), new DefaultThreadFactory("acceptor"));
        this.bootstrap = new ServerBootstrap();
        this.bootstrap.group(this.eventLoop);
        this.bootstrap.channel(NioServerSocketChannel.class);
        this.bootstrap.childHandler(new ChannelInitializer<SocketChannel>() {
            @Override
            protected void initChannel(SocketChannel channel) throws Exception {
                final ChannelPipeline p = channel.pipeline();
                p.addLast("encoder", getEncoder().get());
                p.addLast("decoder", getDecoder().get());
                p.addLast("transport", new NioTransport<>(messageType, AbstractNioAcceptor.this));
            }
        });
        this.bootstrap.option(ChannelOption.SO_BACKLOG, configuration.getSoBacklog());
        this.bootstrap.option(ChannelOption.SO_REUSEADDR, configuration.isSoReuseAddr());
        this.bootstrap.childOption(ChannelOption.TCP_NODELAY, configuration.isTcpNoDelay());
        this.bootstrap.childOption(ChannelOption.SO_KEEPALIVE, configuration.isSoKeepAlive());

        //
        if (configuration.getSoLinger() > 0)
            this.bootstrap.childOption(ChannelOption.SO_LINGER, configuration.getSoLinger());
        if (configuration.isPoolingEnabled()) {
            this.bootstrap.childOption(ChannelOption.ALLOCATOR, new PooledByteBufAllocator(configuration.isPreferDirect()));
        } else {
            this.bootstrap.childOption(ChannelOption.ALLOCATOR, new UnpooledByteBufAllocator(configuration.isPreferDirect()));
        }
        if (configuration.getSoSendBufferSize() > 0)
            this.bootstrap.childOption(ChannelOption.SO_SNDBUF, configuration.getSoSendBufferSize());
        if (configuration.getSoRecvBufferSize() > 0)
            this.bootstrap.childOption(ChannelOption.SO_RCVBUF, configuration.getSoRecvBufferSize());
        bootstrap.childOption(WRITE_BUFFER_WATER_MARK, new WriteBufferWaterMark(configuration.getSoSendBufferLowWaterMark(), configuration.getSoSendBufferHighWaterMark()));
    }

    @Override
    public CompletableFuture<?> shutdown() {
        return new ListenableChannelFuture<>(eventLoop.shutdownGracefully());
    }

    @Override
    public CompletableFuture<Void> connect(String host, int port) {
        if (host == null) return new ListenableChannelFuture<>(this.bootstrap.bind(port));
        else return new ListenableChannelFuture<>(this.bootstrap.bind(host, port));
    }
}

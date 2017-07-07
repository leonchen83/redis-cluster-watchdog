package com.moilioncircle.replicator.cluster.util.net.initiator;

import com.moilioncircle.replicator.cluster.util.concurrent.future.CompletableFuture;
import com.moilioncircle.replicator.cluster.util.concurrent.future.ListenableChannelFuture;
import com.moilioncircle.replicator.cluster.util.concurrent.future.ListenableFuture;
import com.moilioncircle.replicator.cluster.util.net.transport.NioTransport;
import com.moilioncircle.replicator.cluster.util.net.transport.TransportListener;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.redis.RedisMessage;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.concurrent.TimeUnit;

import static com.sun.javafx.animation.TickCalculation.toMillis;

/**
 * Created by Baoyi Chen on 2017/7/7.
 */
public class NioInitiator extends AbstractNioInitiator<RedisMessage> {

    public static final Log logger = LogFactory.getLog(NioInitiator.class);

    protected final Bootstrap bootstrap;
    protected EventLoopGroup workerGroup;
    protected volatile NioTransport<RedisMessage> transport;
    protected final InitiatorConfiguration configuration;

    public NioInitiator(InitiatorConfiguration configuration) {
        this.configuration = configuration;
        this.bootstrap = new Bootstrap();
        this.bootstrap.channel(NioSocketChannel.class);
        this.bootstrap.handler(new ChannelInitializer<SocketChannel>() {
            @Override
            protected void initChannel(SocketChannel channel) throws Exception {
                final ChannelPipeline p = channel.pipeline();
                p.addLast("encoder", getEncoder().get());
                p.addLast("decoder", getDecoder().get());
                p.addLast("transport", transport = new Handler(NioInitiator.this));
            }
        });
        this.bootstrap.option(ChannelOption.TCP_NODELAY, configuration.isTcpNoDelay());
        this.bootstrap.option(ChannelOption.SO_KEEPALIVE, configuration.isSoKeepAlive());
        this.bootstrap.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, (int) configuration.getConnectTimeout());
        this.bootstrap.group(workerGroup != null ? workerGroup : (workerGroup = new NioEventLoopGroup()));
        if (configuration.getSoSendBufferSize() > 0)
            this.bootstrap.option(ChannelOption.SO_SNDBUF, configuration.getSoSendBufferSize());
        if (configuration.getSoRecvBufferSize() > 0)
            this.bootstrap.option(ChannelOption.SO_RCVBUF, configuration.getSoRecvBufferSize());
    }

    protected void reconnect(long delay, CompletableFuture<Void> r, String host, int port) {
        this.bootstrap.config().group().schedule(() -> connect(r, host, port), delay, TimeUnit.MILLISECONDS);
    }

    @Override
    public CompletableFuture<Void> connect(String host, int port) {
        CompletableFuture<Void> r = new ListenableFuture<>();
        connect(r, host, port);
        return r;
    }

    protected void connect(CompletableFuture<Void> r, String host, int port) {
        final ChannelFutureListener v = new ConnectFutureListenerImpl(r, host, port);
        ChannelFuture f = this.bootstrap.connect(host, port);
        f.addListener(v);
    }

    @Override
    public CompletableFuture<?> shutdown() {
        return new ListenableChannelFuture<>(workerGroup.shutdownGracefully());
    }

    private static class Handler extends NioTransport<RedisMessage> {
        public Handler(TransportListener<RedisMessage> listener) {
            super(listener);
        }
    }

    private class ConnectFutureListenerImpl implements ChannelFutureListener {
        //
        private final int port;
        private final String host;
        private final long mark = System.nanoTime();
        private final CompletableFuture<Void> future;

        private ConnectFutureListenerImpl(CompletableFuture<Void> future, String host, int port) {
            this.future = future;
            this.host = host;
            this.port = port;
        }

        @Override
        public void operationComplete(ChannelFuture f) throws Exception {
            final long et = System.nanoTime() - mark;
            if (!f.isSuccess()) {
                if (configuration.isAutoReconnect())
                    reconnect(configuration.getReconnectInterval(), future, host, port);
                else future.failure(f.cause());
            } else {
                future.success(null);
                logger.info("connected to host: " + host + ", port: " + port + ", elapsed time: " + toMillis(et) + " ms");
            }
        }
    }
}

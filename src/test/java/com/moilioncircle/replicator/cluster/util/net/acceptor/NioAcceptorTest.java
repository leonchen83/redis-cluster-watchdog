package com.moilioncircle.replicator.cluster.util.net.acceptor;

import com.moilioncircle.replicator.cluster.util.net.transport.Transport;
import com.moilioncircle.replicator.cluster.util.net.transport.TransportListener;
import io.netty.handler.codec.redis.RedisDecoder;
import io.netty.handler.codec.redis.RedisEncoder;
import io.netty.handler.codec.redis.RedisMessage;

import java.util.concurrent.ExecutionException;

/**
 * Created by Baoyi Chen on 2017/7/7.
 */
public class NioAcceptorTest {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        AcceptorConfiguration configuration = new AcceptorConfiguration();
        NioAcceptor server = new NioAcceptor(configuration);
        server.setEncoder(RedisEncoder::new);
        server.setDecoder(RedisDecoder::new);
        server.setTransportListener(new TransportListener<RedisMessage>() {
            @Override
            public void onConnected(Transport<RedisMessage> transport) {
                System.out.println("> " + transport.toString());
            }

            @Override
            public void onMessage(Transport<RedisMessage> transport, RedisMessage message) {
                System.out.println(message);
            }

            @Override
            public void onException(Transport<RedisMessage> transport, Throwable cause) {
                System.out.println("cause:" + cause.getMessage());
            }

            @Override
            public void onDisconnected(Transport<RedisMessage> transport, Throwable cause) {
                System.out.println("< " + transport.toString());
            }
        });
        server.bind(null, 6379).get();
    }

}
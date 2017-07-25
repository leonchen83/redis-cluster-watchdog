package com.moilioncircle.redis.cluster.watchdog.util.net.initiator;

import com.moilioncircle.redis.cluster.watchdog.util.net.NetworkConfiguration;
import com.moilioncircle.redis.cluster.watchdog.util.net.NioBootstrapImpl;
import com.moilioncircle.redis.cluster.watchdog.util.net.transport.Transport;
import com.moilioncircle.redis.cluster.watchdog.util.net.transport.TransportListener;
import io.netty.handler.codec.redis.RedisDecoder;
import io.netty.handler.codec.redis.RedisEncoder;
import io.netty.handler.codec.redis.RedisMessage;

import java.util.concurrent.ExecutionException;

/**
 * @author Leon Chen
 * @since 1.0.0
 */
public class NioInitiatorTest {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        NetworkConfiguration configuration = NetworkConfiguration.defaultSetting();
        NioBootstrapImpl<RedisMessage> client = new NioBootstrapImpl<>(false, configuration);
        client.setEncoder(RedisEncoder::new);
        client.setDecoder(RedisDecoder::new);
        client.setup();
        client.setTransportListener(new TransportListener<RedisMessage>() {
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
        client.connect("127.0.0.1", 6379).get();
    }
}
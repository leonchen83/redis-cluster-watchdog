package com.moilioncircle.redis.cluster.watchdog.codec;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

/**
 * @author Leon Chen
 * @since 1.0.0
 */
public class RedisEncoder extends MessageToByteEncoder<byte[]> {
    @Override
    protected void encode(ChannelHandlerContext ctx, byte[] cmd, ByteBuf out) throws Exception {
        out.writeBytes(cmd);
    }
}

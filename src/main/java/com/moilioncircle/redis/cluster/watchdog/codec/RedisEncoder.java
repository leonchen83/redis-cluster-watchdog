package com.moilioncircle.redis.cluster.watchdog.codec;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

/**
 * @author Leon Chen
 * @since 1.0.0
 */
public class RedisEncoder extends MessageToByteEncoder<Object> {
    @Override
    protected void encode(ChannelHandlerContext ctx, Object cmd, ByteBuf out) throws Exception {
        if (cmd instanceof byte[]) {
            out.writeBytes((byte[]) cmd);
        }
    }
}
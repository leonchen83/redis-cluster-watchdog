package com.moilioncircle.replicator.cluster.codec;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

/**
 * Created by Baoyi Chen on 2017/7/14.
 */
public class RedisEncoder extends MessageToByteEncoder<Object> {
    @Override
    protected void encode(ChannelHandlerContext ctx, Object cmd, ByteBuf out) throws Exception {
        if (cmd instanceof byte[]) {
            out.writeBytes((byte[]) cmd);
        }
    }
}

package com.moilioncircle.replicator.cluster.codec;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;

import java.nio.charset.Charset;
import java.util.List;

import static java.lang.Integer.parseInt;

/**
 * Created by Baoyi Chen on 2017/7/14.
 */
public class RedisDecoder extends ByteToMessageDecoder {

    public static final Charset CHARSET = Charset.forName("UTF-8");

    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
        in.markReaderIndex();
        try {
            Object request = decode(ctx, in);
            if (request instanceof byte[]) {
                out.add(new byte[][]{(byte[]) request});
            } else if (request instanceof byte[][]) {
                out.add(request);
            }
        } catch (Exception e) {
            in.resetReaderIndex();
        }
    }

    protected Object decode(ChannelHandlerContext ctx, ByteBuf in) {
        int c = in.readByte(), index, len;
        byte[] rs;
        switch (c) {
            case '$':
                //RESP Bulk Strings
                for (len = 0, index = in.readerIndex(); ; ) {
                    while (in.readByte() != '\r') len++;
                    if (in.readByte() != '\n') len++;
                    else break;
                }
                len = parseInt((String) in.getCharSequence(index, len, CHARSET));
                if (len == -1) return null;
                rs = new byte[len];
                in.readBytes(rs);
                if (in.readByte() != '\r') ctx.close();
                if (in.readByte() != '\n') ctx.close();
                return rs;
            case ':':
                // RESP Integers
                for (len = 0, index = in.readerIndex(); ; ) {
                    while (in.readByte() != '\r') len++;
                    if (in.readByte() != '\n') len++;
                    else break;
                }
                rs = new byte[len];
                return in.getBytes(index, rs);
            case '*':
                // RESP Arrays
                for (len = 0, index = in.readerIndex(); ; ) {
                    while (in.readByte() != '\r') len++;
                    if (in.readByte() != '\n') len++;
                    else break;
                }
                len = parseInt((String) in.getCharSequence(index, len, CHARSET));
                if (len == -1) return null;
                byte[][] ary = new byte[len][];
                for (int i = 0; i < len; i++) ary[i] = (byte[]) decode(ctx, in);
                return ary;
            case '+':
                // RESP Simple Strings
                for (len = 0, index = in.readerIndex(); ; ) {
                    while (in.readByte() != '\r') len++;
                    if (in.readByte() != '\n') len++;
                    else break;
                }
                rs = new byte[len];
                in.getBytes(index, rs);
                return rs;
            case '-':
                // RESP Errors
                for (len = 0, index = in.readerIndex(); ; ) {
                    while (in.readByte() != '\r') len++;
                    if (in.readByte() != '\n') len++;
                    else break;
                }
                rs = new byte[len];
                in.getBytes(index, rs);
                return rs;
            default:
                ctx.close();
                return null;
        }
    }
}

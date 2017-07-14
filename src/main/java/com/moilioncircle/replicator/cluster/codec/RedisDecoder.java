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

    public static final AssertionError ASSERTION_EXCEPTION = new AssertionError();

    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
        in.markReaderIndex();
        try {
            String request = decode(in, in.readerIndex());
            if (request != null) out.add(request);
        } catch (Exception e) {
            in.resetReaderIndex();
        }
    }

    protected String decode(ByteBuf in, int start) {
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
                len = parseInt((String) in.getCharSequence(index, len, Charset.forName("UTF-8")));
                if (len == -1) return null;
                rs = new byte[len];
                in.readBytes(rs);
                if (in.readByte() != '\r') throw ASSERTION_EXCEPTION;
                if (in.readByte() != '\n') throw ASSERTION_EXCEPTION;
                return (String) in.getCharSequence(start, in.readerIndex() - start, Charset.forName("UTF-8"));
            case ':':
                // RESP Integers
                for (len = 0; ; ) {
                    while (in.readByte() != '\r') len++;
                    if (in.readByte() != '\n') len++;
                    else break;
                }
                return (String) in.getCharSequence(start, in.readerIndex() - start, Charset.forName("UTF-8"));
            case '*':
                // RESP Arrays
                for (len = 0, index = in.readerIndex(); ; ) {
                    while (in.readByte() != '\r') len++;
                    if (in.readByte() != '\n') len++;
                    else break;
                }
                len = parseInt((String) in.getCharSequence(index, len, Charset.forName("UTF-8")));
                if (len == -1)
                    return (String) in.getCharSequence(start, in.readerIndex() - start, Charset.forName("UTF-8"));
                String r = null;
                for (int i = 0; i < len; i++) r = decode(in, start);
                return r;
            case '+':
                // RESP Simple Strings
                for (len = 0; ; ) {
                    while (in.readByte() != '\r') len++;
                    if (in.readByte() != '\n') len++;
                    else break;
                }
                return (String) in.getCharSequence(start, in.readerIndex() - start, Charset.forName("UTF-8"));
            case '-':
                // RESP Errors
                for (len = 0; ; ) {
                    while (in.readByte() != '\r') len++;
                    if (in.readByte() != '\n') len++;
                    else break;
                }
                return (String) in.getCharSequence(start, in.readerIndex() - start, Charset.forName("UTF-8"));
            default:
                throw ASSERTION_EXCEPTION;
        }
    }
}

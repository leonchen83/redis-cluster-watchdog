/*
 * Copyright 2016-2018 Leon Chen
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

package com.moilioncircle.redis.cluster.watchdog.codec;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;

import java.util.List;

/**
 * @author Leon Chen
 * @since 1.0.0
 */
public class RedisDecoder extends ByteToMessageDecoder {
    
    public static long parseLong(ByteBuf in) {
        long v = 0;
        int sign = 1;
        int read = in.readByte();
        if (read == '-') {
            read = in.readByte();
            sign = -1;
        }
        do {
            if (read == '\r' && in.readByte() == '\n') break;
            int value = read - '0';
            if (value >= 0 && value < 10) v = v * 10 + value;
            else throw new NumberFormatException("Invalid character in integer");
            read = in.readByte();
        } while (true);
        return v * sign;
    }
    
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
        in.markReaderIndex();
        try {
            Object request = decode(ctx, in);
            if (request instanceof byte[][]) out.add(request);
            else if (request instanceof byte[]) out.add(new byte[][]{(byte[]) request});
        } catch (Exception e) {
            in.resetReaderIndex();
        }
    }
    
    protected Object decode(ChannelHandlerContext ctx, ByteBuf in) {
        int c = in.readByte(), index, v;
        byte[] rs;
        switch (c) {
            case '$':
                //RESP Bulk Strings
                v = (int) parseLong(in);
                if (v == -1) return null;
                rs = new byte[v];
                in.readBytes(rs);
                if (in.readByte() != '\r') ctx.close();
                if (in.readByte() != '\n') ctx.close();
                return rs;
            case ':':
                // RESP Integers
                for (v = 0, index = in.readerIndex(); ; ) {
                    while (in.readByte() != '\r') v++;
                    if (in.readByte() != '\n') v++;
                    else break;
                }
                rs = new byte[v];
                in.getBytes(index, rs);
                return rs;
            case '*':
                // RESP Arrays
                v = (int) parseLong(in);
                if (v == -1) return null;
                byte[][] ary = new byte[v][];
                for (int i = 0; i < v; i++) ary[i] = (byte[]) decode(ctx, in);
                return ary;
            case '+':
                // RESP Simple Strings
                for (v = 0, index = in.readerIndex(); ; ) {
                    while (in.readByte() != '\r') v++;
                    if (in.readByte() != '\n') v++;
                    else break;
                }
                rs = new byte[v];
                in.getBytes(index, rs);
                return rs;
            case '-':
                // RESP Errors
                for (v = 0, index = in.readerIndex(); ; ) {
                    while (in.readByte() != '\r') v++;
                    if (in.readByte() != '\n') v++;
                    else break;
                }
                rs = new byte[v];
                in.getBytes(index, rs);
                return rs;
            default:
                ctx.close();
                return null;
        }
    }
}

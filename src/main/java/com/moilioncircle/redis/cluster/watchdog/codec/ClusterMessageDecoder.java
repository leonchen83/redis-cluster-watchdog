package com.moilioncircle.redis.cluster.watchdog.codec;

import com.moilioncircle.redis.cluster.watchdog.message.*;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.moilioncircle.redis.cluster.watchdog.ClusterConstants.*;

/**
 * @author Leon Chen
 * @since 1.0.0
 */
public class ClusterMessageDecoder extends ByteToMessageDecoder {
    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
        ClusterMessage msg = decode(in);
        if (msg == null) return;
        out.add(msg);
    }

    protected ClusterMessage decode(ByteBuf in) {
        in.markReaderIndex();
        try {
            ClusterMessage hdr = new ClusterMessage();
            hdr.signature = (String) in.readCharSequence(4, CHARSET);
            hdr.length = in.readInt();
            if (in.readableBytes() < hdr.length - 8) {
                in.resetReaderIndex();
                return null;
            }
            hdr.version = in.readUnsignedShort();
            hdr.port = in.readUnsignedShort();
            hdr.type = in.readUnsignedShort();
            hdr.count = in.readUnsignedShort();
            hdr.currentEpoch = in.readLong();
            hdr.configEpoch = in.readLong();
            hdr.offset = in.readLong();
            hdr.name = truncate(in, CLUSTER_NODE_NULL_NAME);
            in.readBytes(hdr.slots);
            hdr.master = truncate(in, CLUSTER_NODE_NULL_NAME);
            hdr.ip = truncate(in, CLUSTER_NODE_NULL_IP);
            in.readBytes(hdr.reserved);
            hdr.busPort = in.readUnsignedShort();
            hdr.flags = in.readUnsignedShort();
            hdr.state = in.readByte();
            in.readBytes(hdr.messageFlags);
            switch (hdr.type) {
                case CLUSTERMSG_TYPE_PING:
                case CLUSTERMSG_TYPE_PONG:
                case CLUSTERMSG_TYPE_MEET:
                    hdr.data = new ClusterMessageData();
                    hdr.data.gossips = new ArrayList<>();
                    for (int i = 0; i < hdr.count; i++) {
                        ClusterMessageDataGossip gossip = new ClusterMessageDataGossip();
                        gossip.name = truncate(in, CLUSTER_NODE_NULL_NAME);
                        gossip.pingTime = in.readInt() * 1000L;
                        gossip.pongTime = in.readInt() * 1000L;
                        gossip.ip = truncate(in, CLUSTER_NODE_NULL_IP);
                        gossip.port = in.readUnsignedShort();
                        gossip.busPort = in.readUnsignedShort();
                        gossip.flags = in.readUnsignedShort();
                        in.readBytes(gossip.reserved);
                        hdr.data.gossips.add(gossip);
                    }
                    break;
                case CLUSTERMSG_TYPE_FAIL:
                    hdr.data = new ClusterMessageData();
                    hdr.data.fail = new ClusterMessageDataFail();
                    hdr.data.fail.name = truncate(in, CLUSTER_NODE_NULL_NAME);
                    break;
                case CLUSTERMSG_TYPE_PUBLISH:
                    hdr.data = new ClusterMessageData();
                    hdr.data.publish = new ClusterMessageDataPublish();
                    hdr.data.publish.channelLength = in.readInt();
                    hdr.data.publish.messageLength = in.readInt();
                    in.readBytes(hdr.data.publish.bulkData);
                    break;
                case CLUSTERMSG_TYPE_UPDATE:
                    hdr.data = new ClusterMessageData();
                    hdr.data.config = new ClusterMessageDataUpdate();
                    hdr.data.config.configEpoch = in.readLong();
                    hdr.data.config.name = truncate(in, CLUSTER_NODE_NULL_NAME);
                    in.readBytes(hdr.data.config.slots);
                    break;
                default:
                    break;
            }
            return hdr;
        } catch (Exception e) {
            in.resetReaderIndex();
            return null;
        }
    }

    public String truncate(ByteBuf in, byte[] bytes) {
        byte[] ary = new byte[bytes.length];
        in.readBytes(ary);
        if (Arrays.equals(ary, bytes)) return null;
        for (int i = 0; i < ary.length; i++) {
            if (ary[i] != 0) continue;
            return new String(ary, 0, i, CHARSET);
        }
        return new String(ary, CHARSET);
    }
}

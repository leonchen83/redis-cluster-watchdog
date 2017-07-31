package com.moilioncircle.redis.cluster.watchdog.codec;

import com.moilioncircle.redis.cluster.watchdog.Version;
import com.moilioncircle.redis.cluster.watchdog.message.ClusterMessage;
import com.moilioncircle.redis.cluster.watchdog.message.ClusterMessageDataGossip;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;

import java.util.Arrays;
import java.util.List;

import static com.moilioncircle.redis.cluster.watchdog.ClusterConstants.*;
import static com.moilioncircle.redis.cluster.watchdog.ClusterState.valueOf;
import static com.moilioncircle.redis.cluster.watchdog.Version.PROTOCOL_V0;
import static com.moilioncircle.redis.cluster.watchdog.Version.PROTOCOL_V1;
import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * @author Leon Chen
 * @since 1.0.0
 */
public class ClusterMessageDecoder extends ByteToMessageDecoder {
    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
        ClusterMessage msg = decode(in);
        if (msg != null) out.add(msg);
    }

    protected ClusterMessage decode(ByteBuf in) {
        in.markReaderIndex();
        try {
            ClusterMessage hdr = new ClusterMessage();
            hdr.signature = (String) in.readCharSequence(4, UTF_8);
            hdr.length = in.readInt();
            if (in.readableBytes() < hdr.length - 8) {
                in.resetReaderIndex(); return null;
            }
            hdr.version = Version.valueOf(in.readUnsignedShort());
            if (hdr.version == PROTOCOL_V0) decodeMessageV0(hdr, in);
            else if (hdr.version == PROTOCOL_V1) decodeMessageV1(hdr, in);
            else throw new UnsupportedOperationException("version: " + hdr.version);
            return hdr;
        } catch (Exception e) {
            in.resetReaderIndex(); return null;
        }
    }

    protected void decodeMessageV0(ClusterMessage hdr, ByteBuf in) {
        in.skipBytes(2);
        hdr.type = in.readUnsignedShort();
        hdr.count = in.readUnsignedShort();
        hdr.currentEpoch = in.readLong();
        hdr.configEpoch = in.readLong();
        hdr.offset = in.readLong();
        hdr.name = truncate(in, CLUSTER_NODE_NULL_NAME);
        in.readBytes(hdr.slots);
        hdr.master = truncate(in, CLUSTER_NODE_NULL_NAME);
        in.skipBytes(32);
        hdr.ip = null;
        hdr.port = in.readUnsignedShort();
        hdr.busPort = hdr.port + CLUSTER_PORT_INCR;
        hdr.flags = in.readUnsignedShort();
        hdr.state = valueOf(in.readByte());
        in.readBytes(hdr.messageFlags);
        switch (hdr.type) {
            case CLUSTERMSG_TYPE_PING:
            case CLUSTERMSG_TYPE_PONG:
            case CLUSTERMSG_TYPE_MEET:
                for (int i = 0; i < hdr.count; i++) {
                    ClusterMessageDataGossip gossip = new ClusterMessageDataGossip();
                    gossip.name = truncate(in, CLUSTER_NODE_NULL_NAME);
                    gossip.pingTime = in.readInt() * 1000L;
                    gossip.pongTime = in.readInt() * 1000L;
                    gossip.ip = truncate(in, CLUSTER_NODE_NULL_IP);
                    gossip.port = in.readUnsignedShort();
                    gossip.busPort = gossip.port + CLUSTER_PORT_INCR;
                    gossip.flags = in.readUnsignedShort();
                    in.skipBytes(2);
                    in.skipBytes(4);
                    hdr.data.gossips.add(gossip);
                }
                break;
            case CLUSTERMSG_TYPE_FAIL:
                hdr.data.fail.name = truncate(in, CLUSTER_NODE_NULL_NAME);
                break;
            case CLUSTERMSG_TYPE_PUBLISH:
                hdr.data.publish.channelLength = in.readInt();
                hdr.data.publish.messageLength = in.readInt();
                in.readBytes(hdr.data.publish.bulkData);
                break;
            case CLUSTERMSG_TYPE_UPDATE:
                hdr.data.config.configEpoch = in.readLong();
                hdr.data.config.name = truncate(in, CLUSTER_NODE_NULL_NAME);
                in.readBytes(hdr.data.config.slots);
                break;
            default:
                break;
        }
    }

    protected void decodeMessageV1(ClusterMessage hdr, ByteBuf in) {
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
        in.skipBytes(34);
        hdr.busPort = in.readUnsignedShort();
        hdr.flags = in.readUnsignedShort();
        hdr.state = valueOf(in.readByte());
        in.readBytes(hdr.messageFlags);
        switch (hdr.type) {
            case CLUSTERMSG_TYPE_PING:
            case CLUSTERMSG_TYPE_PONG:
            case CLUSTERMSG_TYPE_MEET:
                for (int i = 0; i < hdr.count; i++) {
                    ClusterMessageDataGossip gossip = new ClusterMessageDataGossip();
                    gossip.name = truncate(in, CLUSTER_NODE_NULL_NAME);
                    gossip.pingTime = in.readInt() * 1000L;
                    gossip.pongTime = in.readInt() * 1000L;
                    gossip.ip = truncate(in, CLUSTER_NODE_NULL_IP);
                    gossip.port = in.readUnsignedShort();
                    gossip.busPort = in.readUnsignedShort();
                    gossip.flags = in.readUnsignedShort();
                    in.skipBytes(4);
                    hdr.data.gossips.add(gossip);
                }
                break;
            case CLUSTERMSG_TYPE_FAIL:
                hdr.data.fail.name = truncate(in, CLUSTER_NODE_NULL_NAME);
                break;
            case CLUSTERMSG_TYPE_PUBLISH:
                hdr.data.publish.channelLength = in.readInt();
                hdr.data.publish.messageLength = in.readInt();
                in.readBytes(hdr.data.publish.bulkData);
                break;
            case CLUSTERMSG_TYPE_UPDATE:
                hdr.data.config.configEpoch = in.readLong();
                hdr.data.config.name = truncate(in, CLUSTER_NODE_NULL_NAME);
                in.readBytes(hdr.data.config.slots);
                break;
            default:
                break;
        }
    }

    public String truncate(ByteBuf in, byte[] bytes) {
        byte[] ary = new byte[bytes.length];
        in.readBytes(ary);
        if (Arrays.equals(ary, bytes)) return null;
        for (int i = 0; i < ary.length; i++) {
            if (ary[i] != 0) continue;
            return new String(ary, 0, i, UTF_8);
        }
        return new String(ary, UTF_8);
    }
}

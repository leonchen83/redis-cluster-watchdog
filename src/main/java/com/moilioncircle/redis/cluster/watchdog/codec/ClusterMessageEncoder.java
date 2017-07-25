package com.moilioncircle.redis.cluster.watchdog.codec;

import com.moilioncircle.redis.cluster.watchdog.message.ClusterMessage;
import com.moilioncircle.redis.cluster.watchdog.message.ClusterMessageDataGossip;
import com.moilioncircle.redis.cluster.watchdog.message.RCmbMessage;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

import java.util.Arrays;

import static com.moilioncircle.redis.cluster.watchdog.ClusterConstants.*;

/**
 * @author Leon Chen
 * @since 1.0.0
 */
public class ClusterMessageEncoder extends MessageToByteEncoder<RCmbMessage> {

    @Override
    protected void encode(ChannelHandlerContext ctx, RCmbMessage msg, ByteBuf out) throws Exception {
        if (!(msg instanceof ClusterMessage)) return;
        ClusterMessage hdr = (ClusterMessage) msg;
        out.writeBytes(hdr.signature.getBytes());
        switch (hdr.type) {
            case CLUSTERMSG_TYPE_PING:
            case CLUSTERMSG_TYPE_PONG:
            case CLUSTERMSG_TYPE_MEET:
                out.writeInt(2256 + hdr.count * 104);
                break;
            case CLUSTERMSG_TYPE_FAIL:
                out.writeInt(2296);
                break;
            case CLUSTERMSG_TYPE_PUBLISH:
                out.writeInt(2272);
                break;
            case CLUSTERMSG_TYPE_UPDATE:
                out.writeInt(4352);
                break;
            default:
                out.writeInt(2256);
                break;
        }
        out.writeShort(hdr.version);
        out.writeShort(hdr.port);
        out.writeShort(hdr.type);
        out.writeShort(hdr.count);
        out.writeLong(hdr.currentEpoch);
        out.writeLong(hdr.configEpoch);
        out.writeLong(hdr.offset);
        out.writeBytes(extract(hdr.name, CLUSTER_NODE_NULL_NAME));
        out.writeBytes(hdr.slots);
        out.writeBytes(extract(hdr.master, CLUSTER_NODE_NULL_NAME));
        out.writeBytes(extract(hdr.ip, CLUSTER_NODE_NULL_IP));
        out.writeBytes(hdr.reserved);
        out.writeShort(hdr.busPort);
        out.writeShort(hdr.flags);
        out.writeByte(hdr.state);
        out.writeBytes(hdr.messageFlags);
        switch (hdr.type) {
            case CLUSTERMSG_TYPE_PING:
            case CLUSTERMSG_TYPE_PONG:
            case CLUSTERMSG_TYPE_MEET:
                for (int i = 0; i < hdr.count; i++) {
                    ClusterMessageDataGossip gossip = hdr.data.gossips.get(i);
                    out.writeBytes(extract(gossip.name, CLUSTER_NODE_NULL_NAME));
                    out.writeInt((int) (gossip.pingTime / 1000L));
                    out.writeInt((int) (gossip.pongTime / 1000L));
                    out.writeBytes(extract(gossip.ip, CLUSTER_NODE_NULL_IP));
                    out.writeShort(gossip.port);
                    out.writeShort(gossip.busPort);
                    out.writeShort(gossip.flags);
                    out.writeBytes(gossip.reserved);
                }
                break;
            case CLUSTERMSG_TYPE_FAIL:
                out.writeBytes(extract(hdr.data.fail.name, CLUSTER_NODE_NULL_NAME));
                break;
            case CLUSTERMSG_TYPE_PUBLISH:
                out.writeInt(hdr.data.publish.channelLength);
                out.writeInt(hdr.data.publish.messageLength);
                out.writeBytes(hdr.data.publish.bulkData);
                break;
            case CLUSTERMSG_TYPE_UPDATE:
                out.writeLong(hdr.data.config.configEpoch);
                out.writeBytes(extract(hdr.data.config.name, CLUSTER_NODE_NULL_NAME));
                out.writeBytes(hdr.data.config.slots);
                break;
            default:
                break;
        }
    }

    public byte[] extract(String str, byte[] bytes) {
        if (str == null) return bytes;
        byte[] extracted = str.getBytes();
        if (extracted.length == bytes.length) return extracted;
        return Arrays.copyOf(extracted, bytes.length);
    }
}

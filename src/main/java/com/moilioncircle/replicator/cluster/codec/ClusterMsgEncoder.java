package com.moilioncircle.replicator.cluster.codec;

import com.moilioncircle.replicator.cluster.message.ClusterMsg;
import com.moilioncircle.replicator.cluster.message.Message;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

import java.util.Arrays;

import static com.moilioncircle.replicator.cluster.ClusterConstants.*;

/**
 * Created by Baoyi Chen on 2017/7/12.
 */
public class ClusterMsgEncoder extends MessageToByteEncoder<Message> {

    @Override
    protected void encode(ChannelHandlerContext ctx, Message msg, ByteBuf out) throws Exception {
        if (!(msg instanceof ClusterMsg)) return;
        ClusterMsg hdr = (ClusterMsg) msg;
        out.writeBytes(hdr.sig.getBytes());
        if (hdr.type == CLUSTERMSG_TYPE_PING || hdr.type == CLUSTERMSG_TYPE_PONG || hdr.type == CLUSTERMSG_TYPE_MEET) {
            int len = hdr.count * (40 + 4 + 4 + 46 + 2 + 2 + 2 + 4);
            out.writeInt(2256 + len);
        } else if (hdr.type == CLUSTERMSG_TYPE_FAIL) {
            out.writeInt(2256 + 40);
        } else if (hdr.type == CLUSTERMSG_TYPE_PUBLISH) {
            out.writeInt(2256 + 4 + 4 + 8);
        } else if (hdr.type == CLUSTERMSG_TYPE_UPDATE) {
            out.writeInt(2256 + 8 + 40 + 2048);
        } else {
            out.writeInt(2256);
        }
        out.writeShort(hdr.ver);
        out.writeShort(hdr.port);
        out.writeShort(hdr.type);
        out.writeShort(hdr.count);
        out.writeLong(hdr.currentEpoch);
        out.writeLong(hdr.configEpoch);
        out.writeLong(hdr.offset);
        out.writeBytes(hdr.sender == null ? CLUSTER_NODE_NULL_NAME : hdr.sender.getBytes());
        out.writeBytes(hdr.myslots);
        out.writeBytes(hdr.slaveof == null ? CLUSTER_NODE_NULL_NAME : hdr.slaveof.getBytes());
        out.writeBytes(hdr.myip == null ? CLUSTER_NODE_NULL_IP : Arrays.copyOf(hdr.myip.getBytes(), 46));
        out.writeBytes(hdr.notused);
        out.writeShort(hdr.cport);
        out.writeShort(hdr.flags);
        out.writeByte(hdr.state);
        out.writeBytes(hdr.mflags);
        if (hdr.type == CLUSTERMSG_TYPE_PING || hdr.type == CLUSTERMSG_TYPE_PONG || hdr.type == CLUSTERMSG_TYPE_MEET) {
            for (int i = 0; i < hdr.count; i++) {
                out.writeBytes(hdr.data.gossip.get(i).nodename == null ? CLUSTER_NODE_NULL_NAME : hdr.data.gossip.get(i).nodename.getBytes());
                out.writeInt((int) (hdr.data.gossip.get(i).pingSent / 1000));
                out.writeInt((int) (hdr.data.gossip.get(i).pongReceived / 1000));
                out.writeBytes(hdr.data.gossip.get(i).ip == null ? CLUSTER_NODE_NULL_IP : Arrays.copyOf(hdr.data.gossip.get(i).ip.getBytes(), 46));
                out.writeShort(hdr.data.gossip.get(i).port);
                out.writeShort(hdr.data.gossip.get(i).cport);
                out.writeShort(hdr.data.gossip.get(i).flags);
                out.writeBytes(hdr.data.gossip.get(i).notused1);
            }
        } else if (hdr.type == CLUSTERMSG_TYPE_FAIL) {
            out.writeBytes(hdr.data.about.nodename == null ? CLUSTER_NODE_NULL_NAME : hdr.data.about.nodename.getBytes());
        } else if (hdr.type == CLUSTERMSG_TYPE_PUBLISH) {
            out.writeInt(hdr.data.msg.channelLen);
            out.writeInt(hdr.data.msg.messageLen);
            out.writeBytes(hdr.data.msg.bulkData);
        } else if (hdr.type == CLUSTERMSG_TYPE_UPDATE) {
            out.writeLong(hdr.data.nodecfg.configEpoch);
            out.writeBytes(hdr.data.nodecfg.nodename == null ? CLUSTER_NODE_NULL_NAME : hdr.data.nodecfg.nodename.getBytes());
            out.writeBytes(hdr.data.nodecfg.slots);
        }
    }
}

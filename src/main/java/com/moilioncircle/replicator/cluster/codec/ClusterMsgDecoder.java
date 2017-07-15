package com.moilioncircle.replicator.cluster.codec;

import com.moilioncircle.replicator.cluster.message.*;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;

import java.nio.charset.Charset;
import java.util.List;

import static com.moilioncircle.replicator.cluster.ClusterConstants.*;

/**
 * Created by Baoyi Chen on 2017/7/12.
 */
public class ClusterMsgDecoder extends ByteToMessageDecoder {
    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
        ClusterMsg msg = decode(in);
        if (msg != null) {
            System.out.println("decode:" + msg);
            out.add(msg);
        }
    }

    protected ClusterMsg decode(ByteBuf in) {
        in.markReaderIndex();
        try {
            ClusterMsg msg = new ClusterMsg();
            msg.sig = (String) in.readCharSequence(4, Charset.forName("UTF-8"));
            msg.totlen = in.readInt();
            msg.ver = in.readShort();
            msg.port = in.readShort();
            msg.type = in.readShort();
            msg.count = in.readShort();
            msg.currentEpoch = in.readLong();
            msg.configEpoch = in.readLong();
            msg.offset = in.readLong();
            String sender = (String) in.readCharSequence(40, Charset.forName("UTF-8"));
            if (!sender.equals(CLUSTER_NODE_NULL_NAME)) {
                msg.sender = sender;
            }
            byte[] slots = new byte[CLUSTER_SLOTS / 8];
            in.readBytes(slots);
            msg.myslots = slots;
            String slaveof = (String) in.readCharSequence(40, Charset.forName("UTF-8"));
            if (!sender.equals(CLUSTER_NODE_NULL_NAME)) {
                msg.slaveof = slaveof;
            }
            msg.myip = (String) in.readCharSequence(46, Charset.forName("UTF-8"));
            byte[] notused = new byte[34];
            in.readBytes(notused);
            msg.notused = notused;
            msg.cport = in.readShort();
            msg.flags = in.readShort();
            msg.state = in.readByte();
            byte[] mflags = new byte[3];
            in.readBytes(mflags);
            msg.mflags = mflags;
            if (msg.type == CLUSTERMSG_TYPE_PING || msg.type == CLUSTERMSG_TYPE_PONG || msg.type == CLUSTERMSG_TYPE_MEET) {
                msg.data = new ClusterMsgData();
                msg.data.gossip = new ClusterMsgDataGossip[msg.count];
                for (int i = 0; i < msg.data.gossip.length; i++) {
                    msg.data.gossip[i] = new ClusterMsgDataGossip();
                    String nodename = (String) in.readCharSequence(40, Charset.forName("UTF-8"));
                    if (!nodename.equals(CLUSTER_NODE_NULL_NAME)) {
                        msg.data.gossip[i].nodename = nodename;
                    }

                    msg.data.gossip[i].pingSent = in.readInt() * 1000;
                    msg.data.gossip[i].pongReceived = in.readInt() * 1000;
                    msg.data.gossip[i].ip = (String) in.readCharSequence(46, Charset.forName("UTF-8"));
                    msg.data.gossip[i].port = in.readShort();
                    msg.data.gossip[i].cport = in.readShort();
                    msg.data.gossip[i].flags = in.readShort();
                    byte[] notused1 = new byte[32];
                    in.readBytes(notused1);
                    msg.data.gossip[i].notused1 = notused1;
                }
            } else if (msg.type == CLUSTERMSG_TYPE_FAIL) {
                msg.data = new ClusterMsgData();
                msg.data.about = new ClusterMsgDataFail();
                String nodename = (String) in.readCharSequence(40, Charset.forName("UTF-8"));
                if (!nodename.equals(CLUSTER_NODE_NULL_NAME)) {
                    msg.data.about.nodename = nodename;
                }

            } else if (msg.type == CLUSTERMSG_TYPE_PUBLISH) {
                msg.data = new ClusterMsgData();
                msg.data.msg = new ClusterMsgDataPublish();
                msg.data.msg.channelLen = in.readInt();
                msg.data.msg.messageLen = in.readInt();
                byte[] bulkData = new byte[8];
                in.readBytes(bulkData);
                msg.data.msg.bulkData = bulkData;
            } else if (msg.type == CLUSTERMSG_TYPE_UPDATE) {
                msg.data = new ClusterMsgData();
                msg.data.nodecfg = new ClusterMsgDataUpdate();
                msg.data.nodecfg.configEpoch = in.readLong();
                String nodename = (String) in.readCharSequence(40, Charset.forName("UTF-8"));
                if (!nodename.equals(CLUSTER_NODE_NULL_NAME)) {
                    msg.data.nodecfg.nodename = nodename;
                }
                slots = new byte[CLUSTER_SLOTS / 8];
                in.readBytes(slots);
                msg.data.nodecfg.slots = slots;
            }
            return msg;
        } catch (Exception e) {
            in.resetReaderIndex();
            return null;
        }
    }
}

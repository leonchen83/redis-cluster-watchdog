/*
 * Copyright 2016 leon chen
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

package com.moilioncircle.replicator.cluster.gossip;

import com.moilioncircle.replicator.cluster.ClusterLink;
import com.moilioncircle.replicator.cluster.ClusterNode;
import com.moilioncircle.replicator.cluster.message.ClusterMsg;
import com.moilioncircle.replicator.cluster.message.ClusterMsgDataGossip;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static com.moilioncircle.replicator.cluster.ClusterConstants.*;

/**
 * @author Leon Chen
 * @since 2.1.0
 */
public class ClusterMsgManager {

    private static final Log logger = LogFactory.getLog(ClusterMsgManager.class);
    private Server server;
    private ThinGossip gossip;
    private ClusterNode myself;

    public ClusterMsgManager(ThinGossip gossip) {
        this.gossip = gossip;
        this.server = gossip.server;
        this.myself = gossip.myself;
    }

    public void clusterSendMessage(ClusterLink link, ClusterMsg hdr) {
        link.fd.send(hdr);
        int type = hdr.type;
        if (type < CLUSTERMSG_TYPE_COUNT)
            server.cluster.statsBusMessagesSent[type]++;
    }

    public void clusterBroadcastMessage(ClusterMsg hdr) {
        for (ClusterNode node : server.cluster.nodes.values()) {
            if (node.link == null) continue;
            if ((node.flags & (CLUSTER_NODE_MYSELF | CLUSTER_NODE_HANDSHAKE)) != 0) continue;
            clusterSendMessage(node.link, hdr);
        }
    }

    public ClusterMsg clusterBuildMessageHdr(int type) {
        ClusterMsg hdr = new ClusterMsg();
        ClusterNode master = (nodeIsSlave(myself) && myself.slaveof != null) ? myself.slaveof : myself;
        hdr.ver = CLUSTER_PROTO_VER;
        hdr.sig = "RCmb";
        hdr.type = type;
        hdr.sender = myself.name;
        if (server.clusterAnnounceIp != null) {
            hdr.myip = server.clusterAnnounceIp;
        }

        int announcedPort = server.clusterAnnouncePort != 0 ? server.clusterAnnouncePort : server.port;
        int announcedCport = server.clusterAnnounceBusPort != 0 ? server.clusterAnnounceBusPort : (server.port + CLUSTER_PORT_INCR);
        hdr.myslots = master.slots;
        if (master.slaveof != null) {
            hdr.slaveof = myself.slaveof.name;
        }

        hdr.port = announcedPort;
        hdr.cport = announcedCport;
        hdr.flags = myself.flags;
        hdr.state = server.cluster.state;

        hdr.currentEpoch = server.cluster.currentEpoch;
        hdr.configEpoch = master.configEpoch;

        long offset = 0;
        if (nodeIsSlave(myself))
            offset = gossip.replicationGetSlaveOffset();
        else
            offset = server.masterReplOffset;
        hdr.offset = offset;

        if (nodeIsMaster(myself) && server.cluster.mfEnd != 0)
            hdr.mflags[0] |= CLUSTERMSG_FLAG0_PAUSED;

        // TODO hdr.totlen = xx;
        return hdr;
    }

    public boolean clusterNodeIsInGossipSection(ClusterMsg hdr, int count, ClusterNode n) {
        for (int i = 0; i < count; i++) {
            if (hdr.data.gossip[i].nodename.equals(n.name)) return true;
        }
        return false;
    }

    public void clusterSetGossipEntry(ClusterMsg hdr, int i, ClusterNode n) {
        ClusterMsgDataGossip gossip = hdr.data.gossip[i];
        gossip.nodename = n.name;
        gossip.pingSent = n.pingSent / 1000;
        gossip.pongReceived = n.pongReceived / 1000;
        gossip.ip = n.ip;
        gossip.port = n.port;
        gossip.cport = n.cport;
        gossip.flags = n.flags;
        gossip.notused1 = 0;
    }

    public void clusterSendPing(ClusterLink link, int type) {
        int gossipcount = 0;

        int freshnodes = server.cluster.nodes.size() - 2; //去掉当前节点和发送的目标节点

        int wanted = server.cluster.nodes.size() / 10;
        if (wanted < 3) wanted = 3;
        if (wanted > freshnodes) wanted = freshnodes;

        int pfailWanted = (int) server.cluster.statsPfailNodes;

        if (link.node != null && type == CLUSTERMSG_TYPE_PING)
            link.node.pingSent = System.currentTimeMillis();
        ClusterMsg hdr = clusterBuildMessageHdr(type);

        int maxiterations = wanted * 3;
        //不对所有节点发送消息，选取<=wanted个节点
        while (freshnodes > 0 && gossipcount < wanted && maxiterations-- > 0) {
            List<ClusterNode> list = new ArrayList<>(server.cluster.nodes.values());
            int idx = new Random().nextInt(list.size());
            ClusterNode t = list.get(idx);

            if (t.equals(myself)) continue;

            if ((t.flags & CLUSTER_NODE_PFAIL) != 0) continue;

            if ((t.flags & (CLUSTER_NODE_HANDSHAKE | CLUSTER_NODE_NOADDR)) != 0 || (t.link == null && t.numslots == 0)) {
//                freshnodes--;
                continue;
            }

            if (clusterNodeIsInGossipSection(hdr, gossipcount, t)) continue;

            clusterSetGossipEntry(hdr, gossipcount, t);
            freshnodes--;
            gossipcount++;
        }

        //把pfail节点加到最后
        if (pfailWanted != 0) {
            List<ClusterNode> list = new ArrayList<>(server.cluster.nodes.values());
            for (int i = 0; i < list.size() && pfailWanted > 0; i++) {
                ClusterNode node = list.get(i);
                if ((node.flags & CLUSTER_NODE_HANDSHAKE) != 0) continue;
                if ((node.flags & CLUSTER_NODE_NOADDR) != 0) continue;
                if ((node.flags & CLUSTER_NODE_PFAIL) == 0) continue;
                clusterSetGossipEntry(hdr, gossipcount, node);
                freshnodes--;
                gossipcount++;
                pfailWanted--;
            }
        }

        hdr.count = gossipcount;
        //TODO hdr.totlen = xx
        clusterSendMessage(link, hdr);
    }

    public void clusterBroadcastPong(int target) {
        for (ClusterNode node : server.cluster.nodes.values()) {
            if (node.link == null) continue;
            if (node.equals(myself) || nodeInHandshake(node)) continue;
            if (target == CLUSTER_BROADCAST_LOCAL_SLAVES) {
                //node.slaveof.equals(myself)这个条件有点问题
                boolean localSlave = nodeIsSlave(node) && node.slaveof != null && (node.slaveof.equals(myself) || node.slaveof.equals(myself.slaveof));
                if (!localSlave) continue;
            }
            clusterSendPing(node.link, CLUSTERMSG_TYPE_PONG);
        }
    }

    public void clusterSendFail(String nodename) {
        ClusterMsg hdr = clusterBuildMessageHdr(CLUSTERMSG_TYPE_FAIL);
        hdr.data.about.nodename = nodename;
        clusterBroadcastMessage(hdr);
    }

    public void clusterSendUpdate(ClusterLink link, ClusterNode node) {
        if (link == null) return;
        ClusterMsg hdr = clusterBuildMessageHdr(CLUSTERMSG_TYPE_UPDATE);
        hdr.data.nodecfg.nodename = node.name;
        hdr.data.nodecfg.configEpoch = node.configEpoch;
        hdr.data.nodecfg.slots = node.slots;
        clusterSendMessage(link, hdr);
    }

    public void clusterSendMFStart(ClusterNode node) {
        if (node.link == null) return;
        ClusterMsg hdr = clusterBuildMessageHdr(CLUSTERMSG_TYPE_MFSTART);
        clusterSendMessage(node.link, hdr);
    }

    // 一对
    public void clusterRequestFailoverAuth() {
        ClusterMsg hdr = clusterBuildMessageHdr(CLUSTERMSG_TYPE_FAILOVER_AUTH_REQUEST);
        if (server.cluster.mfEnd > 0) hdr.mflags[0] |= CLUSTERMSG_FLAG0_FORCEACK;
        clusterBroadcastMessage(hdr);
    }

    public void clusterSendFailoverAuthIfNeeded(ClusterNode node, ClusterMsg request) {

        long requestCurrentEpoch = request.currentEpoch;

        //只有持有1个以上slot的master有投票权
        if (nodeIsSlave(myself) || myself.numslots == 0) return;

        if (requestCurrentEpoch < server.cluster.currentEpoch) {
            logger.warn("Failover auth denied to " + node.name + ": reqEpoch (" + requestCurrentEpoch + ") < curEpoch(" + server.cluster.currentEpoch + ")");
            return;
        }

        //已经投完票的直接返回
        if (server.cluster.lastVoteEpoch == server.cluster.currentEpoch) {
            logger.warn("Failover auth denied to " + node.name + ": already voted for epoch " + server.cluster.currentEpoch);
            return;
        }

        ClusterNode master = node.slaveof;
        boolean forceAck = (request.mflags[0] & CLUSTERMSG_FLAG0_FORCEACK) != 0;
        //目标node是master 或者 目标node的master不明确 或者在非手动模式下目标node的master没挂都返回
        if (nodeIsMaster(node) || master == null || (!nodeFailed(master) && !forceAck)) {
            if (nodeIsMaster(node)) {
                logger.warn("Failover auth denied to " + node.name + ": it is a master node");
            } else if (master == null) {
                logger.warn("Failover auth denied to " + node.name + ": I don't know its master");
            } else if (!nodeFailed(master)) {
                logger.warn("Failover auth denied to " + node.name + ": its master is up");
            }
            return;
        }

        //这里要求目标node 是slave, 目标node的master != null,目标node的master没挂的情况下必须forceAck为true

        //这里是投票超时的情况
        if (System.currentTimeMillis() - node.slaveof.votedTime < server.clusterNodeTimeout * 2) {
            logger.warn("Failover auth denied to " + node.name + ": can't vote about this master before " + (server.clusterNodeTimeout * 2 - System.currentTimeMillis() - node.slaveof.votedTime) + " milliseconds");
            return;
        }

        byte[] claimedSlots = request.myslots;
        long requestConfigEpoch = request.configEpoch;
        for (int i = 0; i < CLUSTER_SLOTS; i++) {
            if (!gossip.slotManger.bitmapTestBit(claimedSlots, i)) continue;
            // 检测本地slot的configEpoch必须要小于等于远程的, 否则认为本地较新, 终止投票
            if (server.cluster.slots[i] == null || server.cluster.slots[i].configEpoch <= requestConfigEpoch) {
                continue;
            }
            logger.warn("Failover auth denied to " + node.name + ": slot " + i + " epoch (" + server.cluster.slots[i].configEpoch + ") > reqEpoch (" + requestConfigEpoch + ")");
            return;
        }

        clusterSendFailoverAuth(node);
        server.cluster.lastVoteEpoch = server.cluster.currentEpoch;
        node.slaveof.votedTime = System.currentTimeMillis();
        logger.warn("Failover auth granted to " + node.name + " for epoch " + server.cluster.currentEpoch);
    }

    public void clusterSendFailoverAuth(ClusterNode node) {
        if (node.link == null) return;
        ClusterMsg hdr = clusterBuildMessageHdr(CLUSTERMSG_TYPE_FAILOVER_AUTH_ACK);
        clusterSendMessage(node.link, hdr);
    }

    // 发送 failover request ,接收failover ack
    // CLUSTERMSG_TYPE_FAILOVER_AUTH_REQUEST -> CLUSTERMSG_TYPE_FAILOVER_AUTH_ACK

}

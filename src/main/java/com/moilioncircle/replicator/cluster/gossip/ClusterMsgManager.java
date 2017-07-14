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
import java.util.concurrent.ThreadLocalRandom;

import static com.moilioncircle.replicator.cluster.ClusterConstants.*;

/**
 * @author Leon Chen
 * @since 2.1.0
 */
public class ClusterMsgManager {

    private static final Log logger = LogFactory.getLog(ClusterMsgManager.class);
    private Server server;
    private ThinGossip gossip;

    public ClusterMsgManager(ThinGossip gossip) {
        this.gossip = gossip;
        this.server = gossip.server;
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
        ClusterNode master = (nodeIsSlave(server.myself) && server.myself.slaveof != null) ? server.myself.slaveof : server.myself;
        hdr.ver = CLUSTER_PROTO_VER;
        hdr.sig = "RCmb";
        hdr.type = type;
        hdr.sender = server.myself.name;
        hdr.myip = gossip.configuration.getClusterAnnounceIp();

        hdr.myslots = master.slots;
        if (master.slaveof != null) {
            hdr.slaveof = server.myself.slaveof.name;
        }

        hdr.flags = server.myself.flags;
        hdr.port = gossip.configuration.getClusterAnnouncePort();
        hdr.cport = gossip.configuration.getClusterAnnounceBusPort();
        hdr.state = server.cluster.state;

        hdr.currentEpoch = server.cluster.currentEpoch;
        hdr.configEpoch = master.configEpoch;

        if (nodeIsSlave(server.myself))
            hdr.offset = gossip.replicationManager.replicationGetSlaveOffset();
        else
            logger.warn("myself must be a slave");
        hdr.totlen = 100;
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
        //选取<=wanted个节点加到gossip消息体里
        while (freshnodes > 0 && gossipcount < wanted && maxiterations-- > 0) {
            List<ClusterNode> list = new ArrayList<>(server.cluster.nodes.values());
            int idx = ThreadLocalRandom.current().nextInt(list.size());
            ClusterNode t = list.get(idx);

            if (t.equals(server.myself)) continue;

            if ((t.flags & CLUSTER_NODE_PFAIL) != 0) continue;

            if ((t.flags & (CLUSTER_NODE_HANDSHAKE | CLUSTER_NODE_NOADDR)) != 0 || (t.link == null && t.numslots == 0))
                continue;

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
                if (nodeInHandshake(node) || nodeWithoutAddr(node) || !nodePFailed(node)) continue;
                clusterSetGossipEntry(hdr, gossipcount, node);
                freshnodes--;
                gossipcount++;
                pfailWanted--;
            }
        }

        hdr.count = gossipcount;
        hdr.totlen = 100;
        clusterSendMessage(link, hdr);
    }

    public void clusterBroadcastPong(int target) {
        for (ClusterNode node : server.cluster.nodes.values()) {
            if (node.link == null) continue;
            if (node.equals(server.myself) || nodeInHandshake(node)) continue;
            if (target == CLUSTER_BROADCAST_LOCAL_SLAVES) {
                //node.slaveof.equals(myself)这个条件有点问题
                boolean localSlave = nodeIsSlave(node) && node.slaveof != null && (node.slaveof.equals(server.myself) || node.slaveof.equals(server.myself.slaveof));
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

    // 发送 failover request ,接收failover ack
    // CLUSTERMSG_TYPE_FAILOVER_AUTH_REQUEST -> CLUSTERMSG_TYPE_FAILOVER_AUTH_ACK

}

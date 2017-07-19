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

package com.moilioncircle.replicator.cluster.manager;

import com.moilioncircle.replicator.cluster.message.ClusterMessage;
import com.moilioncircle.replicator.cluster.message.ClusterMessageData;
import com.moilioncircle.replicator.cluster.message.ClusterMessageDataGossip;
import com.moilioncircle.replicator.cluster.state.ClusterLink;
import com.moilioncircle.replicator.cluster.state.ClusterNode;
import com.moilioncircle.replicator.cluster.state.ServerState;
import com.moilioncircle.replicator.cluster.state.States;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadLocalRandom;

import static com.moilioncircle.replicator.cluster.ClusterConstants.*;

/**
 * @author Leon Chen
 * @since 2.1.0
 */
public class ClusterMessageManager {

    private static final Log logger = LogFactory.getLog(ClusterMessageManager.class);
    private ServerState server;
    private ClusterManagers managers;

    public ClusterMessageManager(ClusterManagers managers) {
        this.managers = managers;
        this.server = managers.server;
    }

    public void clusterSendMessage(ClusterLink link, ClusterMessage hdr) {
        try {
            link.fd.send(hdr).get();
            int type = hdr.type;
            if (type < CLUSTERMSG_TYPE_COUNT)
                server.cluster.statsBusMessagesSent[type]++;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (ExecutionException e) {
            logger.warn("send msg error, link: " + link + ",hdr:" + hdr);
        }
    }

    public void clusterBroadcastMessage(ClusterMessage hdr) {
        server.cluster.nodes.values().stream().
                filter(x -> x.link != null && (x.flags & (CLUSTER_NODE_MYSELF | CLUSTER_NODE_HANDSHAKE)) == 0).
                forEach(x -> clusterSendMessage(x.link, hdr));
    }

    public ClusterMessage clusterBuildMessageHdr(int type) {
        ClusterMessage hdr = new ClusterMessage();
        ClusterNode master = (States.nodeIsSlave(server.myself) && server.myself.slaveof != null) ? server.myself.slaveof : server.myself;
        hdr.ver = CLUSTER_PROTO_VER;
        hdr.sig = "RCmb";
        hdr.type = type;
        hdr.sender = server.myself.name;
        hdr.myip = managers.configuration.getClusterAnnounceIp();

        hdr.myslots = master.slots;
        if (server.myself.slaveof != null) {
            hdr.slaveof = server.myself.slaveof.name;
        }

        hdr.flags = server.myself.flags;
        hdr.port = managers.configuration.getClusterAnnouncePort();
        hdr.cport = managers.configuration.getClusterAnnounceBusPort();
        hdr.state = server.cluster.state;

        hdr.currentEpoch = server.cluster.currentEpoch;
        hdr.configEpoch = master.configEpoch;

        if (States.nodeIsSlave(server.myself))
            hdr.offset = managers.replications.replicationGetSlaveOffset();
        return hdr;
    }

    public boolean clusterNodeIsInGossipSection(ClusterMessage hdr, int count, ClusterNode n) {
        return hdr.data.gossip.stream().limit(count).anyMatch(x -> x.nodename.equals(n.name));
    }

    public void clusterSetGossipEntry(ClusterMessage hdr, int i, ClusterNode n) {
        ClusterMessageDataGossip gossip = new ClusterMessageDataGossip();
        gossip.nodename = n.name;
        gossip.pingSent = n.pingSent / 1000;
        gossip.pongReceived = n.pongReceived / 1000;
        gossip.ip = n.ip;
        gossip.port = n.port;
        gossip.cport = n.cport;
        gossip.flags = n.flags;
        gossip.notused1 = new byte[4];
        hdr.data.gossip.add(gossip);
    }

    public void clusterSendPing(ClusterLink link, int type) {
        int freshnodes = server.cluster.nodes.size() - 2;

        int wanted = server.cluster.nodes.size() / 10;
        if (wanted < 3) wanted = 3;
        if (wanted > freshnodes) wanted = freshnodes;

        int pfailWanted = (int) server.cluster.statsPfailNodes;

        if (link.node != null && type == CLUSTERMSG_TYPE_PING)
            link.node.pingSent = System.currentTimeMillis();
        ClusterMessage hdr = clusterBuildMessageHdr(type);
        hdr.data = new ClusterMessageData();
        int maxiterations = wanted * 3;
        int gossipcount = 0;
        while (freshnodes > 0 && gossipcount < wanted && maxiterations-- > 0) {
            List<ClusterNode> list = new ArrayList<>(server.cluster.nodes.values());
            ClusterNode t = list.get(ThreadLocalRandom.current().nextInt(list.size()));

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
                if (States.nodeInHandshake(node) || States.nodeWithoutAddr(node) || !States.nodePFailed(node)) continue;
                clusterSetGossipEntry(hdr, gossipcount, node);
                freshnodes--;
                gossipcount++;
                pfailWanted--;
            }
        }

        hdr.count = gossipcount;
        clusterSendMessage(link, hdr);
    }

    public void clusterSendFail(String nodename) {
        ClusterMessage hdr = clusterBuildMessageHdr(CLUSTERMSG_TYPE_FAIL);
        hdr.data.about.nodename = nodename;
        clusterBroadcastMessage(hdr);
    }

    public void clusterSendUpdate(ClusterLink link, ClusterNode node) {
        if (link == null) return;
        ClusterMessage hdr = clusterBuildMessageHdr(CLUSTERMSG_TYPE_UPDATE);
        hdr.data.nodecfg.nodename = node.name;
        hdr.data.nodecfg.configEpoch = node.configEpoch;
        hdr.data.nodecfg.slots = node.slots;
        clusterSendMessage(link, hdr);
    }

    public void clusterSendFailoverAuth(ClusterNode node) {
        if (node.link == null) return;
        ClusterMessage hdr = clusterBuildMessageHdr(CLUSTERMSG_TYPE_FAILOVER_AUTH_ACK);
        clusterSendMessage(node.link, hdr);
    }

}

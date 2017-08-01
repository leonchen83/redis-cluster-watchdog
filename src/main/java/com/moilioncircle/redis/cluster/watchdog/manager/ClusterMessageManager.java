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

package com.moilioncircle.redis.cluster.watchdog.manager;

import com.moilioncircle.redis.cluster.watchdog.ClusterConfiguration;
import com.moilioncircle.redis.cluster.watchdog.message.ClusterMessage;
import com.moilioncircle.redis.cluster.watchdog.message.ClusterMessageDataGossip;
import com.moilioncircle.redis.cluster.watchdog.state.ClusterLink;
import com.moilioncircle.redis.cluster.watchdog.state.ClusterNode;
import com.moilioncircle.redis.cluster.watchdog.state.NodeStates;
import com.moilioncircle.redis.cluster.watchdog.state.ServerState;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.function.Predicate;

import static com.moilioncircle.redis.cluster.watchdog.ClusterConstants.*;
import static com.moilioncircle.redis.cluster.watchdog.Version.PROTOCOL_V0;
import static com.moilioncircle.redis.cluster.watchdog.Version.PROTOCOL_V1;
import static com.moilioncircle.redis.cluster.watchdog.state.NodeStates.*;
import static java.util.concurrent.ThreadLocalRandom.current;

/**
 * @author Leon Chen
 * @since 1.0.0
 */
public class ClusterMessageManager {

    private static final Log logger = LogFactory.getLog(ClusterMessageManager.class);

    private ServerState server;
    private ClusterManagers managers;
    private ClusterConfiguration configuration;

    public ClusterMessageManager(ClusterManagers managers) {
        this.managers = managers;
        this.server = managers.server;
        this.configuration = managers.configuration;
    }

    public void clusterSetGossipEntry(ClusterMessage hdr, ClusterNode n) {
        ClusterMessageDataGossip g;
        g = new ClusterMessageDataGossip();
        g.flags = n.flags; g.busPort = n.busPort;
        g.pingTime = n.pingTime; g.name = n.name; g.ip = n.ip;
        g.pongTime = n.pongTime; g.port = n.port; hdr.data.gossips.add(g);
    }

    public void clusterSendPing(ClusterLink link, int type) {
        if (configuration.getVersion() == PROTOCOL_V0) clusterSendPingV0(link, type);
        else if (configuration.getVersion() == PROTOCOL_V1) clusterSendPingV1(link, type);
    }

    public void clusterSendFail(String name) {
        ClusterMessage hdr;
        hdr = clusterBuildMessageHdr(CLUSTERMSG_TYPE_FAIL);
        hdr.data.fail.name = name; clusterBroadcastMessage(hdr);
    }

    public void clusterSendUpdate(ClusterLink link, ClusterNode node) {
        if (link == null) return;
        ClusterMessage hdr = clusterBuildMessageHdr(CLUSTERMSG_TYPE_UPDATE);
        hdr.data.config.name = node.name; hdr.data.config.slots = node.slots;
        hdr.data.config.configEpoch = node.configEpoch; clusterSendMessage(link, hdr);
    }

    public void clusterRequestFailoverAuth() {
        clusterBroadcastMessage(clusterBuildMessageHdr(CLUSTERMSG_TYPE_FAILOVER_AUTH_REQUEST));
    }

    public boolean clusterNodeIsInGossipSection(ClusterMessage hdr, int count, ClusterNode n) {
        return hdr.data.gossips.stream().limit(count).anyMatch(e -> Objects.equals(e.name, n.name));
    }

    public void clusterBroadcastMessage(ClusterMessage hdr) {
        Predicate<ClusterNode> t = e -> e.link != null;
        t = t.and(e -> !nodeIsMyself(e.flags) && !nodeInHandshake(e.flags));
        server.cluster.nodes.values().stream().filter(t).forEach(e -> clusterSendMessage(e.link, hdr));
    }

    public ClusterMessage clusterBuildMessageHdr(int type) {
        ClusterNode myself = server.myself;
        boolean isMaster = nodeIsSlave(myself) && myself.master != null;
        ClusterNode myMaster = isMaster ? server.myself.master : myself;
        //
        ClusterMessage hdr = new ClusterMessage();
        hdr.name = myself.name; hdr.flags = myself.flags;
        hdr.busPort = configuration.getClusterAnnounceBusPort();
        if (myself.master != null) hdr.master = myself.master.name;
        hdr.type = type; hdr.signature = "RCmb"; hdr.slots = myMaster.slots;
        hdr.state = server.cluster.state; hdr.configEpoch = myMaster.configEpoch;
        hdr.version = configuration.getVersion(); hdr.ip = configuration.getClusterAnnounceIp();
        if (nodeIsSlave(myself)) hdr.offset = managers.replications.replicationGetSlaveOffset();
        hdr.currentEpoch = server.cluster.currentEpoch; hdr.port = configuration.getClusterAnnouncePort();
        return hdr;
    }

    public void clusterSendMessage(ClusterLink link, ClusterMessage hdr) {
        try {
            link.fd.send(hdr).get();
            if (hdr.type < CLUSTERMSG_TYPE_COUNT)
                server.cluster.messagesSent[hdr.type]++;
        } catch (InterruptedException | ExecutionException e) {
            if (e instanceof InterruptedException) Thread.currentThread().interrupt();
            else logger.error("send RCmb message failed, link: " + link.fd + ",message type:" + hdr.type);
        }
    }

    public void clusterBroadcastPong(int target) {
        ClusterNode myself = server.myself;
        for (ClusterNode node : server.cluster.nodes.values()) {
            if (node.link == null) continue;
            if (Objects.equals(node, myself)) continue;
            if (NodeStates.nodeInHandshake(node)) continue;
            if (target == CLUSTER_BROADCAST_LOCAL_SLAVES) {
                Predicate<ClusterNode> t = e -> nodeIsSlave(e) && e.master != null;
                t = t.and(e -> Objects.equals(e.master, myself) || Objects.equals(e.master, myself.master));
                if (!t.test(node)) continue;
            }
            clusterSendPing(node.link, CLUSTERMSG_TYPE_PONG);
        }
    }

    public void clusterSendFailoverAuth(ClusterNode node) {
        if (node.link != null) clusterSendMessage(node.link, clusterBuildMessageHdr(CLUSTERMSG_TYPE_FAILOVER_AUTH_ACK));
    }

    /**
     *
     */
    private void clusterSendPingV0(ClusterLink link, int type) {
        long now = System.currentTimeMillis();
        int actives = server.cluster.nodes.size() - 2;
        int wanted = server.cluster.nodes.size() / 10;
        wanted = Math.min(Math.max(wanted, 3), actives);
        List<ClusterNode> list = new ArrayList<>(server.cluster.nodes.values());
        if (link.node != null && type == CLUSTERMSG_TYPE_PING) link.node.pingTime = now;

        int max = wanted * 3, gossips = 0;
        ClusterMessage hdr = clusterBuildMessageHdr(type);
        while (actives > 0 && gossips < wanted && max-- > 0) {
            ClusterNode node = list.get(current().nextInt(list.size()));

            if (Objects.equals(node, server.myself)) continue;
            if (max > wanted * 2 && !nodePFailed(node.flags) && !nodeFailed(node.flags)) continue;
            if (NodeStates.nodeWithoutAddr(node.flags)) continue;
            if (NodeStates.nodeInHandshake(node.flags)) continue;
            if (node.link == null && node.assignedSlots == 0) continue;
            if (clusterNodeIsInGossipSection(hdr, gossips, node)) continue;
            clusterSetGossipEntry(hdr, node); actives--; gossips++;
        }
        hdr.count = gossips; clusterSendMessage(link, hdr);
    }

    private void clusterSendPingV1(ClusterLink link, int type) {
        long now = System.currentTimeMillis();
        int actives = server.cluster.nodes.size() - 2;
        int wanted = server.cluster.nodes.size() / 10;
        int fWanted = (int) server.cluster.pFailNodes;
        wanted = Math.min(Math.max(wanted, 3), actives);
        List<ClusterNode> nodes = new ArrayList<>(server.cluster.nodes.values());
        if (link.node != null && type == CLUSTERMSG_TYPE_PING) link.node.pingTime = now;

        int max = wanted * 3, gossips = 0;
        ClusterMessage hdr = clusterBuildMessageHdr(type);
        while (actives > 0 && gossips < wanted && max-- > 0) {
            ClusterNode node = nodes.get(current().nextInt(nodes.size()));

            if (Objects.equals(node, server.myself)) continue;
            if (NodeStates.nodePFailed(node.flags)) continue;
            if (NodeStates.nodeWithoutAddr(node.flags)) continue;
            if (NodeStates.nodeInHandshake(node.flags)) continue;
            if (node.link == null && node.assignedSlots == 0) continue;
            if (clusterNodeIsInGossipSection(hdr, gossips, node)) continue;
            clusterSetGossipEntry(hdr, node); actives--; gossips++;
        }

        if (fWanted != 0) {
            for (int i = 0; i < nodes.size() && fWanted > 0; i++) {
                ClusterNode node = nodes.get(i); if (nodeInHandshake(node)) continue;
                if (nodeWithoutAddr(node)) continue; if (!nodePFailed(node)) continue;
                clusterSetGossipEntry(hdr, node); actives--; gossips++; fWanted--;
            }
        }
        hdr.count = gossips; clusterSendMessage(link, hdr);
    }
}

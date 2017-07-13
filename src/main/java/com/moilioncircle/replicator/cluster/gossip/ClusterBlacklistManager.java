package com.moilioncircle.replicator.cluster.gossip;

import com.moilioncircle.replicator.cluster.ClusterNode;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.AbstractMap;
import java.util.Iterator;
import java.util.Map;

import static com.moilioncircle.replicator.cluster.ClusterConstants.CLUSTER_BLACKLIST_TTL;

/**
 * Created by Baoyi Chen on 2017/7/13.
 */
public class ClusterBlacklistManager {

    private static final Log logger = LogFactory.getLog(ClusterBlacklistManager.class);
    private Server server;
    private ThinGossip1 gossip;
    private ClusterNode myself;

    public ClusterBlacklistManager(ThinGossip1 gossip) {
        this.gossip = gossip;
        this.server = gossip.server;
        this.myself = gossip.myself;
    }

    public void clusterBlacklistCleanup() {
        Iterator<Map.Entry<Long, ClusterNode>> it = server.cluster.nodesBlackList.values().iterator();
        while (it.hasNext()) {
            Map.Entry<Long, ClusterNode> entry = it.next();
            if (entry.getKey() < System.currentTimeMillis()) it.remove();
        }
    }

    public void clusterBlacklistAddNode(ClusterNode node) {
        clusterBlacklistCleanup();
        Map.Entry<Long, ClusterNode> entry = new AbstractMap.SimpleEntry<>(System.currentTimeMillis() + CLUSTER_BLACKLIST_TTL, node);
        server.cluster.nodesBlackList.put(node.name, entry);
    }

    public boolean clusterBlacklistExists(String nodename) {
        clusterBlacklistCleanup();
        return server.cluster.nodesBlackList.containsKey(nodename);
    }
}

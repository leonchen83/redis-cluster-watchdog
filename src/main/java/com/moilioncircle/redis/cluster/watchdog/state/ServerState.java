package com.moilioncircle.redis.cluster.watchdog.state;

import com.moilioncircle.redis.cluster.watchdog.message.RCmbMessage;
import com.moilioncircle.redis.cluster.watchdog.util.net.transport.Transport;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author Leon Chen
 * @since 1.0.0
 */
public class ServerState {
    public int masterPort; public String masterHost;
    public ClusterNode myself; public ClusterState cluster;
    public long iteration = 0; public String previousAddress;
    public long stateSaveTime = 0; public long amongMinorityTime = 0;
    public Map<Transport<RCmbMessage>, ClusterLink> cfd = new ConcurrentHashMap<>();
}

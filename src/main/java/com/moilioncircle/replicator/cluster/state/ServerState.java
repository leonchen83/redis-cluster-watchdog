package com.moilioncircle.replicator.cluster.state;

import com.moilioncircle.replicator.cluster.message.RCmbMessage;
import com.moilioncircle.replicator.cluster.util.net.transport.Transport;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by Baoyi Chen on 2017/7/6.
 */
public class ServerState {
    public int masterPort;
    public String masterHost;
    public ClusterNode myself;
    public ClusterState cluster;
    public Map<Transport<RCmbMessage>, ClusterLink> cfd = new ConcurrentHashMap<>();

    public String prevIp;
    public long iteration = 0;
    public long firstCallTime = 0;
    public long amongMinorityTime = 0;
}

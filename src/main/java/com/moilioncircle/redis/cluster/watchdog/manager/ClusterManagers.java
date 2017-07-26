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

import com.moilioncircle.redis.cluster.watchdog.*;
import com.moilioncircle.redis.cluster.watchdog.state.ServerState;
import com.moilioncircle.redis.replicator.rdb.datatype.KeyValuePair;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

/**
 * @author Leon Chen
 * @since 1.0.0
 */
public class ClusterManagers {

    public ClusterSlotManger slots;
    public ClusterNodeManager nodes;
    public ClusterStateManager states;
    public ClusterConfigManager configs;
    public ClusterMessageManager messages;
    public ReplicationManager replications;
    public ClusterFailoverManager failovers;
    public ClusterConfiguration configuration;
    public ClusterBlacklistManager blacklists;
    public ClusterConnectionManager connections;
    public ClusterCommandHandlerManager commands;
    public ClusterMessageHandlerManager handlers;

    public ExecutorService file;
    public ExecutorService worker;
    public ScheduledExecutorService executor;

    public ClusterWatchdog watchdog;
    public ServerState server = new ServerState();

    private volatile ReplicationListener replicationListener;
    private volatile ClusterStateListener clusterStateListener;
    private volatile ClusterConfigListener clusterConfigListener;
    private volatile RestoreCommandListener restoreCommandListener;

    public ClusterManagers(ClusterConfiguration configuration, ClusterWatchdog watchdog) {
        this.watchdog = watchdog;
        this.configuration = configuration;
        this.slots = new ClusterSlotManger(this);
        this.nodes = new ClusterNodeManager(this);
        this.states = new ClusterStateManager(this);
        this.configs = new ClusterConfigManager(this);
        this.messages = new ClusterMessageManager(this);
        this.replications = new ReplicationManager(this);
        this.failovers = new ClusterFailoverManager(this);
        this.connections = new ClusterConnectionManager();
        this.blacklists = new ClusterBlacklistManager(this);
        this.commands = new ClusterCommandHandlerManager(this);
        this.handlers = new ClusterMessageHandlerManager(this);

        this.file = Executors.newSingleThreadExecutor();
        this.worker = Executors.newSingleThreadExecutor();
        this.executor = Executors.newSingleThreadScheduledExecutor();
    }

    public synchronized ReplicationListener setReplicationListener(ReplicationListener replicationListener) {
        ReplicationListener r = this.replicationListener;
        this.replicationListener = replicationListener;
        return r;
    }

    public synchronized ClusterStateListener setClusterStateListener(ClusterStateListener clusterStateListener) {
        ClusterStateListener r = this.clusterStateListener;
        this.clusterStateListener = clusterStateListener;
        return r;
    }

    public synchronized ClusterConfigListener setClusterConfigListener(ClusterConfigListener clusterConfigListener) {
        ClusterConfigListener r = this.clusterConfigListener;
        this.clusterConfigListener = clusterConfigListener;
        return r;
    }

    public synchronized RestoreCommandListener setReplicationListener(RestoreCommandListener restoreCommandListener) {
        RestoreCommandListener r = this.restoreCommandListener;
        this.restoreCommandListener = restoreCommandListener;
        return r;
    }

    public void notifySetReplication(String ip, int host) {
        worker.submit(() -> {
            ReplicationListener r = this.replicationListener;
            if (r != null) r.onSetReplication(ip, host);
        });
    }

    public void notifyUnsetReplication() {
        worker.submit(() -> {
            ReplicationListener r = this.replicationListener;
            if (r != null) r.onUnsetReplication();
        });
    }

    public long notifyReplicationGetSlaveOffset() {
        ReplicationListener r = this.replicationListener;
        if (r == null) return 0L;
        return r.onGetSlaveOffset();
    }

    public void notifyConfigChanged(ClusterConfigInfo info) {
        worker.submit(() -> {
            ClusterConfigListener r = this.clusterConfigListener;
            if (r != null) r.onConfigChanged(info);
        });
    }

    public void notifyStateChanged(ClusterState state) {
        worker.submit(() -> {
            ClusterStateListener r = this.clusterStateListener;
            if (r != null) r.onStateChanged(state);
        });
    }

    public void notifyRestoreCommand(byte[] key, long ttl, KeyValuePair<?> kv, boolean replace) {
        worker.submit(() -> {
            RestoreCommandListener r = this.restoreCommandListener;
            if (r != null) r.onRestoreCommand(key, ttl, kv, replace);
        });
    }
}

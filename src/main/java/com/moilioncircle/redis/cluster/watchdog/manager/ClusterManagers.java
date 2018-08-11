/*
 * Copyright 2016-2018 Leon Chen
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

import com.moilioncircle.redis.cluster.watchdog.ClusterConfigInfo;
import com.moilioncircle.redis.cluster.watchdog.ClusterConfigListener;
import com.moilioncircle.redis.cluster.watchdog.ClusterConfiguration;
import com.moilioncircle.redis.cluster.watchdog.ClusterNodeInfo;
import com.moilioncircle.redis.cluster.watchdog.ClusterNodeListener;
import com.moilioncircle.redis.cluster.watchdog.ClusterState;
import com.moilioncircle.redis.cluster.watchdog.ClusterStateListener;
import com.moilioncircle.redis.cluster.watchdog.ClusterWatchdog;
import com.moilioncircle.redis.cluster.watchdog.ReplicationListener;
import com.moilioncircle.redis.cluster.watchdog.Resourcable;
import com.moilioncircle.redis.cluster.watchdog.command.CommandHandler;
import com.moilioncircle.redis.cluster.watchdog.state.ServerState;
import com.moilioncircle.redis.cluster.watchdog.storage.DefaultStorageEngine;
import com.moilioncircle.redis.cluster.watchdog.storage.StorageEngine;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * @author Leon Chen
 * @since 1.0.0
 */
public class ClusterManagers implements Resourcable {
    //
    public ServerState server;
    public StorageEngine engine;
    public ExecutorService config;
    public ExecutorService worker;
    public ClusterWatchdog watchdog;
    public ScheduledExecutorService cron;
    //
    public ClusterSlotManager slots;
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
    
    private volatile ClusterNodeListener clusterNodeListener;
    private volatile ReplicationListener replicationListener;
    private volatile ClusterStateListener clusterStateListener;
    private volatile ClusterConfigListener clusterConfigListener;
    
    public ClusterManagers(ClusterConfiguration configuration, ClusterWatchdog watchdog) {
        this.watchdog = watchdog;
        this.server = new ServerState();
        this.configuration = configuration;
        this.engine = new DefaultStorageEngine();
        //
        this.slots = new ClusterSlotManager(this);
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
        //
        this.config = Executors.newSingleThreadExecutor();
        this.worker = Executors.newSingleThreadExecutor();
        this.cron = Executors.newSingleThreadScheduledExecutor();
    }
    
    /**
     *
     */
    public long notifyReplicationGetSlaveOffset() {
        ReplicationListener r = this.replicationListener;
        if (r == null) return 0L;
        return r.onGetSlaveOffset();
    }
    
    public void notifyNodeAdded(ClusterNodeInfo node) {
        ClusterNodeListener r = this.clusterNodeListener;
        worker.submit(() -> {
            if (r != null) r.onNodeAdded(node);
        });
    }
    
    public void notifyNodeDeleted(ClusterNodeInfo node) {
        ClusterNodeListener r = this.clusterNodeListener;
        worker.submit(() -> {
            if (r != null) r.onNodeDeleted(node);
        });
    }
    
    public void notifyNodeFailed(ClusterNodeInfo failed) {
        ClusterNodeListener r = this.clusterNodeListener;
        worker.submit(() -> {
            if (r != null) r.onNodeFailed(failed);
        });
    }
    
    public void notifyConfigChanged(ClusterConfigInfo info) {
        ClusterConfigListener r = this.clusterConfigListener;
        worker.submit(() -> {
            if (r != null) r.onConfigChanged(info);
        });
    }
    
    public void notifyStateChanged(ClusterState state) {
        ClusterStateListener r = this.clusterStateListener;
        worker.submit(() -> {
            if (r != null) r.onStateChanged(state);
        });
    }
    
    public void notifyNodePFailed(ClusterNodeInfo pfailed) {
        ClusterNodeListener r = this.clusterNodeListener;
        worker.submit(() -> {
            if (r != null) r.onNodePFailed(pfailed);
        });
    }
    
    public void notifyUnsetNodeFailed(ClusterNodeInfo failed) {
        ClusterNodeListener r = this.clusterNodeListener;
        worker.submit(() -> {
            if (r != null) r.onUnsetNodeFailed(failed);
        });
    }
    
    public void notifyUnsetReplication(StorageEngine engine) {
        ReplicationListener r = this.replicationListener;
        worker.submit(() -> {
            if (r != null) r.onUnsetReplication(engine);
        });
    }
    
    public void notifyUnsetNodePFailed(ClusterNodeInfo pfailed) {
        ClusterNodeListener r = this.clusterNodeListener;
        worker.submit(() -> {
            if (r != null) r.onUnsetNodePFailed(pfailed);
        });
    }
    
    public void notifySetReplication(String ip, int host, StorageEngine engine) {
        ReplicationListener r = this.replicationListener;
        worker.submit(() -> {
            if (r != null) r.onSetReplication(ip, host, engine);
        });
    }
    
    /**
     *
     */
    public void setStorageEngine(StorageEngine engine) {
        this.engine = engine;
    }
    
    public CommandHandler addCommandHandler(String name, CommandHandler handler) {
        return this.commands.addCommandHandler(name, handler);
    }
    
    public synchronized ClusterNodeListener setClusterNodeListener(ClusterNodeListener clusterNodeListener) {
        ClusterNodeListener r = this.clusterNodeListener;
        this.clusterNodeListener = clusterNodeListener;
        return r;
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
    
    @Override
    public void start() {
        this.engine.start();
    }
    
    @Override
    public void stop() {
        stop(0L, TimeUnit.MILLISECONDS);
    }
    
    @Override
    public void stop(long timeout, TimeUnit unit) {
        // if myself is a slave. safe to shutdown replication socket.
        this.replications.replicationUnsetMaster();
        
        try {
            this.config.shutdown();
            this.config.awaitTermination(timeout, unit);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        
        try {
            this.worker.shutdown();
            this.worker.awaitTermination(timeout, unit);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        this.engine.stop(timeout, unit);
    }
}

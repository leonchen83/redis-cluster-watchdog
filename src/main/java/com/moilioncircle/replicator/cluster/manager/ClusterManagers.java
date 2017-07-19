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

import com.moilioncircle.replicator.cluster.ClusterConfiguration;
import com.moilioncircle.replicator.cluster.state.ServerState;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

/**
 * @author Leon Chen
 * @since 2.1.0
 */
public class ClusterManagers {
    public ExecutorService file;
    public ExecutorService worker;
    public ScheduledExecutorService executor;
    public ServerState server = new ServerState();

    public ClusterSlotManger slots;
    public ClusterNodeManager nodes;
    public ClusterStateManager states;
    public ClusterConfigManager configs;
    public ClusterMessageManager messages;
    public ReplicationManager replications;
    public ClusterConfiguration configuration;
    public ClusterBlacklistManager blacklists;
    public ClusterConnectionManager connections;
    public ClusterMessageHandlerManager handlers;

    public ClusterManagers(ClusterConfiguration configuration) {
        this.file = Executors.newSingleThreadExecutor();
        this.worker = Executors.newSingleThreadExecutor();
        this.executor = Executors.newSingleThreadScheduledExecutor();
        this.configuration = configuration;
        this.configuration.validate();
        this.slots = new ClusterSlotManger(this);
        this.nodes = new ClusterNodeManager(this);
        this.states = new ClusterStateManager(this);
        this.configs = new ClusterConfigManager(this);
        this.messages = new ClusterMessageManager(this);
        this.replications = new ReplicationManager(this);
        this.connections = new ClusterConnectionManager();
        this.blacklists = new ClusterBlacklistManager(this);
        this.handlers = new ClusterMessageHandlerManager(this);
    }
}

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

package com.moilioncircle.redis.cluster.watchdog.command;

import com.moilioncircle.redis.cluster.watchdog.ClusterConfiguration;
import com.moilioncircle.redis.cluster.watchdog.manager.ClusterManagers;
import com.moilioncircle.redis.cluster.watchdog.state.ServerState;
import com.moilioncircle.redis.cluster.watchdog.storage.StorageEngine;

/**
 * @author Leon Chen
 * @since 1.0.0
 */
public abstract class AbstractCommandHandler extends CommandHandler.Adaptor {

    protected final ServerState server;
    protected final ClusterManagers managers;

    public AbstractCommandHandler(ClusterManagers managers) {
        this.managers = managers;
        this.server = managers.server;
    }

    @Override
    public StorageEngine getStorageEngine() {
        return managers.engine;
    }

    @Override
    public void setStorageEngine(StorageEngine storageEngine) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ClusterConfiguration getConfiguration() {
        return managers.configuration;
    }

    @Override
    public void setConfiguration(ClusterConfiguration configuration) {
        throw new UnsupportedOperationException();
    }
}

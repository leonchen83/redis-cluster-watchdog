/*
 * Copyright 2016-2017 Leon Chen
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

package com.moilioncircle.redis.cluster.watchdog;

import java.util.concurrent.TimeUnit;

/**
 * @author Leon Chen
 * @since 1.0.0
 */
public class RedisClusterWatchdog extends AbstractClusterWatchdog {

    protected final Resourcable server;
    protected final Resourcable gossip;

    public RedisClusterWatchdog() {
        this(ClusterConfiguration.defaultSetting());
    }

    public RedisClusterWatchdog(ClusterConfiguration configuration) {
        super(configuration);
        this.server = new ThinServer(managers);
        this.gossip = new ThinGossip(managers);
    }

    @Override
    public void start() {
        Resourcable.startQuietly(managers);
        Resourcable.startQuietly(server);
        Resourcable.startQuietly(gossip);
    }

    @Override
    public void stop() {
        stop(0, TimeUnit.MILLISECONDS);
    }

    @Override
    public void stop(long timeout, TimeUnit unit) {
        try {
            managers.command.shutdown();
            managers.command.awaitTermination(timeout, unit);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        try {
            managers.cron.shutdown();
            managers.cron.awaitTermination(timeout, unit);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        Resourcable.stopQuietly(gossip, timeout, unit);
        Resourcable.stopQuietly(server, timeout, unit);
        Resourcable.stopQuietly(managers, timeout, unit);
    }
}

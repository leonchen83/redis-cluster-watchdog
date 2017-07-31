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

    protected RedisClusterWatchdog(ClusterConfiguration configuration) {
        super(configuration);
        this.server = new ThinServer(managers);
        this.gossip = new ThinGossip(managers);
    }

    @Override
    public void start() {
        server.start();
        gossip.start();
    }

    @Override
    public void stop(long timeout, TimeUnit unit) {
        gossip.stop(timeout, unit);
        server.stop(timeout, unit);
    }
}

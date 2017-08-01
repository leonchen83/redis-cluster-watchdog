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

package com.moilioncircle.redis.cluster.watchdog;

import com.moilioncircle.redis.cluster.watchdog.codec.RedisDecoder;
import com.moilioncircle.redis.cluster.watchdog.codec.RedisEncoder;
import com.moilioncircle.redis.cluster.watchdog.manager.ClusterManagers;
import com.moilioncircle.redis.cluster.watchdog.util.net.NioBootstrapImpl;
import com.moilioncircle.redis.cluster.watchdog.util.net.transport.Transport;
import com.moilioncircle.redis.cluster.watchdog.util.net.transport.TransportListener;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.moilioncircle.redis.cluster.watchdog.ClusterConfigInfo.valueOf;

/**
 * @author Leon Chen
 * @since 1.0.0
 */
public class ThinServer implements Resourcable {

    private static final Log logger = LogFactory.getLog(ThinServer.class);

    private ClusterManagers managers;
    private ClusterConfiguration configuration;
    private volatile NioBootstrapImpl<Object> acceptor;

    public ThinServer(ClusterManagers managers) {
        this.managers = managers;
        this.configuration = managers.configuration;
    }

    @Override
    public void start() {
        acceptor = new NioBootstrapImpl<>();
        acceptor.setEncoder(RedisEncoder::new);
        acceptor.setDecoder(RedisDecoder::new); acceptor.setup();
        acceptor.setTransportListener(new RedisTransportListener());
        try {
            acceptor.connect(null, configuration.getClusterAnnouncePort()).get();
        } catch (InterruptedException | ExecutionException e) {
            if (e instanceof InterruptedException) Thread.currentThread().interrupt();
            else throw new UnsupportedOperationException(e.getCause());
        }
    }

    @Override
    public void stop() {
        stop(0, TimeUnit.MILLISECONDS);
    }

    @Override
    public void stop(long timeout, TimeUnit unit) {
        NioBootstrapImpl<Object> acceptor = this.acceptor;
        try {
            if (acceptor != null) acceptor.shutdown().get(timeout, unit);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (ExecutionException e) {
            logger.error("unexpected error", e.getCause());
        } catch (TimeoutException e) {
            logger.error("stop timeout error", e);
        }
    }

    private class RedisTransportListener implements TransportListener<Object> {
        @Override
        public void onConnected(Transport<Object> t) {
            if (configuration.isVerbose()) logger.info("[acceptor] > " + t);
        }

        @Override
        public void onMessage(Transport<Object> t, Object message) {
            managers.cron.execute(() -> {
                ClusterConfigInfo previous;
                previous = valueOf(managers.server.cluster);
                managers.commands.handleCommand(t, (byte[][]) message);
                ClusterConfigInfo next = valueOf(managers.server.cluster);
                if (!previous.equals(next)) managers.config.submit(() -> managers.configs.clusterSaveConfig(next));
            });
        }

        @Override
        public void onDisconnected(Transport<Object> t, Throwable cause) {
            if (configuration.isVerbose()) logger.info("[acceptor] < " + t);
        }
    }
}

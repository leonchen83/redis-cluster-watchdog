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
import com.moilioncircle.redis.cluster.watchdog.util.net.NioBootstrapConfiguration;
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
public class ThinServer {
    private static final Log logger = LogFactory.getLog(ThinServer.class);

    private ClusterManagers managers;
    private volatile NioBootstrapImpl<Object> cfd;

    public ThinServer(ClusterManagers managers) {
        this.managers = managers;
    }

    public void start() {
        cfd = new NioBootstrapImpl<>(true, NioBootstrapConfiguration.defaultSetting());
        cfd.setEncoder(RedisEncoder::new);
        cfd.setDecoder(RedisDecoder::new);
        cfd.setup();
        cfd.setTransportListener(new TransportListener<Object>() {
            @Override
            public void onConnected(Transport<Object> transport) {
                logger.info("[acceptor] > " + transport);
            }

            @Override
            public void onMessage(Transport<Object> transport, Object message) {
                managers.executor.execute(() -> {
                    ClusterConfigInfo previous = valueOf(managers.server.cluster);
                    managers.commands.handleCommand(transport, (byte[][]) message);
                    ClusterConfigInfo next = valueOf(managers.server.cluster);
                    if (!previous.equals(next))
                        managers.file.submit(() -> managers.configs.clusterSaveConfig(next, false));
                });
            }

            @Override
            public void onDisconnected(Transport<Object> transport, Throwable cause) {
                logger.info("[acceptor] < " + transport);
            }
        });
        try {
            cfd.connect(null, managers.configuration.getClusterAnnouncePort()).get();
        } catch (InterruptedException | ExecutionException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            } else {
                throw new UnsupportedOperationException(e.getCause());
            }
        }
    }

    public void stop(long timeout, TimeUnit unit) {
        NioBootstrapImpl<Object> cfd = this.cfd;
        if (cfd != null) {
            try {
                cfd.shutdown().get(timeout, unit);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (ExecutionException e) {
                logger.error("unexpected error", e.getCause());
            } catch (TimeoutException e) {
                logger.error("stop timeout error", e);
            }
        }
    }
}

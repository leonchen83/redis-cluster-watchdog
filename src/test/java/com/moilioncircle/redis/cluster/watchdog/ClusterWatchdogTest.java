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

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author Leon Chen
 * @since 1.0.0
 */
public class ClusterWatchdogTest {
    private static AtomicBoolean set = new AtomicBoolean(false);

    public static void main(String[] args) throws IOException {
        for (int i = 1; i <= 10; i++) {
            final int j = i;
            new Thread(() -> {
                ClusterConfiguration c = ClusterConfiguration.defaultSetting();
                c.setAsMaster(true);
                c.setClusterAnnouncePort(10000 + j);
                ClusterWatchdog watchdog = new RedisClusterWatchdog(c);
                if (set.compareAndSet(false, true)) {
                    watchdog.setClusterNodeFailedListener(new ClusterNodeFailedListener() {
                        @Override
                        public void onNodePFailed(ClusterNodeInfo pFailed) {
                            System.out.println("set pfailed:" + pFailed.name);
                        }

                        @Override
                        public void onUnsetNodePFailed(ClusterNodeInfo pFailed) {
                            System.out.println("unset pfailed:" + pFailed.name);
                        }

                        @Override
                        public void onNodeFailed(ClusterNodeInfo failed) {
                            System.out.println("set failed:" + failed.name);
                        }

                        @Override
                        public void onUnsetNodeFailed(ClusterNodeInfo failed) {
                            System.out.println("unset failed:" + failed.name);
                        }
                    });
                }
                watchdog.start();
            }).start();
        }
        System.in.read();
    }
}

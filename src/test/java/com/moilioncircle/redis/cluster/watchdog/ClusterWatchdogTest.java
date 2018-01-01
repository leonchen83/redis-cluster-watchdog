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

package com.moilioncircle.redis.cluster.watchdog;

import java.io.IOException;

/**
 * @author Leon Chen
 * @since 1.0.0
 */
public class ClusterWatchdogTest {
    public static void main(String[] args) throws IOException {
        for (int i = 1; i <= 10; i++) {
            final int j = i;
            new Thread(() -> {
                ClusterConfiguration c = ClusterConfiguration.defaultSetting();
                c.setFailover(true).setVersion(Version.PROTOCOL_V1).setClusterAnnouncePort(10000 + j);

                ClusterWatchdog watchdog = new RedisClusterWatchdog(c);
                watchdog.start();
            }).start();
        }
    }
}

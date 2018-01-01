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

package com.moilioncircle.redis.cluster.watchdog.state;

import com.moilioncircle.redis.cluster.watchdog.message.RCmbMessage;
import com.moilioncircle.redis.cluster.watchdog.util.net.transport.Transport;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author Leon Chen
 * @since 1.0.0
 */
public class ServerState {
    public int masterPort; public String masterHost;
    public ClusterNode myself; public ClusterState cluster;
    public long iteration = 0; public String previousAddress;
    public long stateSaveTime = 0; public long amongMinorityTime = 0;
    public Map<Transport<RCmbMessage>, ClusterLink> cfd = new ConcurrentHashMap<>();
}

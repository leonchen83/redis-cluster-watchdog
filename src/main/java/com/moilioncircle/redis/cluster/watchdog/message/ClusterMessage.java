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

package com.moilioncircle.redis.cluster.watchdog.message;

import com.moilioncircle.redis.cluster.watchdog.ClusterState;
import com.moilioncircle.redis.cluster.watchdog.Version;

import static com.moilioncircle.redis.cluster.watchdog.ClusterConstants.CLUSTER_SLOTS_BYTES;

/**
 * @author Leon Chen
 * @since 1.0.0
 */
public class ClusterMessage implements RCmbMessage {
    public int type;
    public int flags;
    public String name;
    public String ip;
    public int port;
    public int busPort;
    public int count;
    public int length;
    public long offset;
    public String master;
    public Version version;
    public String signature;
    public long configEpoch;
    public long currentEpoch;
    public ClusterState state;
    public byte[] messageFlags = new byte[3];
    public byte[] slots = new byte[CLUSTER_SLOTS_BYTES];
    public ClusterMessageData data = new ClusterMessageData();
}

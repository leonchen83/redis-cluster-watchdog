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

import com.moilioncircle.redis.cluster.watchdog.state.ClusterNode;

import java.util.Arrays;
import java.util.Objects;

import static com.moilioncircle.redis.cluster.watchdog.ClusterConstants.CLUSTER_SLOTS_BYTES;

/**
 * @author Leon Chen
 * @since 1.0.0
 */
public class ClusterNodeInfo {

    private long pingTime; private long pongTime;
    private String master; private long configEpoch;
    private byte[] slots = new byte[CLUSTER_SLOTS_BYTES];
    private String ip; private int port; private int busPort;
    private int flags; private String name; private String link;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ClusterNodeInfo nodeInfo = (ClusterNodeInfo) o;

        if (port != nodeInfo.port) return false;
        if (busPort != nodeInfo.busPort) return false;
        if (flags != nodeInfo.flags) return false;
        if (configEpoch != nodeInfo.configEpoch) return false;
        if (!name.equals(nodeInfo.name)) return false;
        if (ip != null ? !ip.equals(nodeInfo.ip) : nodeInfo.ip != null) return false;
        if (master != null ? !master.equals(nodeInfo.master) : nodeInfo.master != null) return false;
        if (link != null ? !link.equals(nodeInfo.link) : nodeInfo.link != null) return false;
        return Arrays.equals(slots, nodeInfo.slots);
    }

    @Override
    public int hashCode() {
        int result = name.hashCode();
        result = 31 * result + (ip != null ? ip.hashCode() : 0);
        result = 31 * result + port;
        result = 31 * result + busPort;
        result = 31 * result + flags;
        result = 31 * result + (master != null ? master.hashCode() : 0);
        result = 31 * result + (int) (configEpoch ^ (configEpoch >>> 32));
        result = 31 * result + (link != null ? link.hashCode() : 0);
        result = 31 * result + Arrays.hashCode(slots);
        return result;
    }

    public static ClusterNodeInfo valueOf(ClusterNode myself) {
        return valueOf(myself, myself);
    }

    public static ClusterNodeInfo valueOf(ClusterNode node, ClusterNode myself) {
        ClusterNodeInfo n = new ClusterNodeInfo();
        n.configEpoch = node.configEpoch;
        n.name = node.name; n.flags = node.flags;
        n.pingTime = node.pingTime; n.pongTime = node.pongTime;
        n.master = node.master == null ? null : node.master.name;
        n.ip = node.ip; n.port = node.port; n.busPort = node.busPort;
        System.arraycopy(node.slots, 0, n.slots, 0, node.slots.length);
        n.link = node.link != null || Objects.equals(node, myself) ? "connected" : "disconnected";
        return n;
    }

    /**
     *
     */
    public int getPort() {
        return port;
    }

    public String getIp() {
        return ip;
    }

    public int getFlags() {
        return flags;
    }

    public int getBusPort() {
        return busPort;
    }

    public String getName() {
        return name;
    }

    public String getLink() {
        return link;
    }

    public byte[] getSlots() {
        return slots;
    }

    public long getPingTime() {
        return pingTime;
    }

    public long getPongTime() {
        return pongTime;
    }

    public String getMaster() {
        return master;
    }

    public long getConfigEpoch() {
        return configEpoch;
    }

    /**
     *
     */
    public void setIp(String ip) {
        this.ip = ip;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public void setFlags(int flags) {
        this.flags = flags;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setLink(String link) {
        this.link = link;
    }

    public void setSlots(byte[] slots) {
        this.slots = slots;
    }

    public void setBusPort(int busPort) {
        this.busPort = busPort;
    }

    public void setMaster(String master) {
        this.master = master;
    }

    public void setPingTime(long pingTime) {
        this.pingTime = pingTime;
    }

    public void setPongTime(long pongTime) {
        this.pongTime = pongTime;
    }

    public void setConfigEpoch(long configEpoch) {
        this.configEpoch = configEpoch;
    }

    @Override
    public String toString() {
        return "Node:[" +
                "address='" + (ip == null ? "0.0.0.0" : ip) + ":" + port + "@" + busPort + '\'' +
                ", flags=" + flags + ", name='" + name + '\'' + ", link='" + link + '\'' +
                ", pingTime=" + pingTime + ", pongTime=" + pongTime + ", master='" + master + '\'' +
                ", configEpoch=" + configEpoch + ']';
    }
}

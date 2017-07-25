package com.moilioncircle.redis.cluster.watchdog.message;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Leon Chen
 * @since 1.0.0
 */
public class ClusterMessageData {
    public List<ClusterMessageDataGossip> gossips = new ArrayList<>();
    public ClusterMessageDataFail fail = new ClusterMessageDataFail();
    public ClusterMessageDataUpdate config = new ClusterMessageDataUpdate();
    public ClusterMessageDataPublish publish = new ClusterMessageDataPublish();

    @Override
    public String toString() {
        return "ClusterMessageData{" +
                "gossips=" + gossips +
                ", fail=" + fail +
                ", publish=" + publish +
                ", config=" + config +
                '}';
    }
}

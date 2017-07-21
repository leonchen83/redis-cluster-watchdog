package com.moilioncircle.redis.cluster.watchdog.message;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Leon Chen
 * @since 1.0.0
 */
public class ClusterMessageData {
    public ClusterMessageDataFail fail;
    public ClusterMessageDataUpdate config;
    public ClusterMessageDataPublish publish;
    public List<ClusterMessageDataGossip> gossips = new ArrayList<>();

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

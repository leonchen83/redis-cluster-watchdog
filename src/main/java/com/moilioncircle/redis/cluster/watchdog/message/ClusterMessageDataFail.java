package com.moilioncircle.redis.cluster.watchdog.message;

/**
 * @author Leon Chen
 * @since 1.0.0
 */
public class ClusterMessageDataFail {
    public String name;

    @Override
    public String toString() {
        return "ClusterMessageDataFail{" +
                "name='" + name + '\'' +
                '}';
    }
}

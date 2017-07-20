package com.moilioncircle.redis.cluster.watchdog.message;

/**
 * Created by Baoyi Chen on 2017/7/6.
 */
public class ClusterMessageDataFail {
    public String nodename;

    @Override
    public String toString() {
        return "ClusterMessageDataFail{" +
                "nodename='" + nodename + '\'' +
                '}';
    }
}

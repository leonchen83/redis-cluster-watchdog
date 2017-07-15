package com.moilioncircle.replicator.cluster.message;

/**
 * Created by Baoyi Chen on 2017/7/6.
 */
public class ClusterMsgDataFail {
    public String nodename;

    @Override
    public String toString() {
        return "ClusterMsgDataFail{" +
                "nodename='" + nodename + '\'' +
                '}';
    }
}

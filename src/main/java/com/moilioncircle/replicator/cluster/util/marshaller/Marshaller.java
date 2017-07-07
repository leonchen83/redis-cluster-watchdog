package com.moilioncircle.replicator.cluster.util.marshaller;

/**
 * Created by Baoyi Chen on 2017/7/6.
 */
public interface Marshaller<T> {
    T read(byte[] bytes);

    byte[] write(T message);
}

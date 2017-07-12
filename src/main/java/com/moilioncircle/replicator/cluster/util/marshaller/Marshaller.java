package com.moilioncircle.replicator.cluster.util.marshaller;

/**
 * @author Leon Chen
 * @since 1.0.0
 */
public interface Marshaller<T> {
    T read(byte[] bytes);

    byte[] write(T message);
}

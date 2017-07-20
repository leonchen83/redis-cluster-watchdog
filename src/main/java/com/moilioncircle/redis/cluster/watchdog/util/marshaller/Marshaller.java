package com.moilioncircle.redis.cluster.watchdog.util.marshaller;

/**
 * @author Leon Chen
 * @since 1.0.0
 */
public interface Marshaller<T> {
    T read(byte[] bytes);

    byte[] write(T message);
}

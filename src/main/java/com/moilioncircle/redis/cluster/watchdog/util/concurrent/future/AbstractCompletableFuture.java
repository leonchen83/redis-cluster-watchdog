package com.moilioncircle.redis.cluster.watchdog.util.concurrent.future;

/**
 * @author Leon Chen
 * @since 1.0.0
 */
public abstract class AbstractCompletableFuture<T> implements CompletableFuture<T> {

    protected volatile FutureListener<T> listener;

    @Override
    public synchronized FutureListener<T> setListener(FutureListener<T> listener) {
        FutureListener<T> r = this.listener;
        this.listener = listener;
        if (isDone() && listener != null) listener.onComplete(this);
        return r;
    }
}

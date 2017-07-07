package com.moilioncircle.replicator.cluster.util.concurrent.future;

/**
 * Created by Baoyi Chen on 2017/7/7.
 */
public abstract class AbstractCompletableFuture<T> implements CompletableFuture<T> {

    protected volatile FutureListener<T> listener;

    @Override
    public synchronized FutureListener<T> setListener(FutureListener<T> listener) {
        FutureListener<T> r = this.listener;
        this.listener = listener;
        if (this.isDone() && listener != null) listener.onComplete(this);
        return r;
    }
}

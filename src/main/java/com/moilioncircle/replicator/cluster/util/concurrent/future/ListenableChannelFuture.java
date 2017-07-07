package com.moilioncircle.replicator.cluster.util.concurrent.future;

import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Created by Baoyi Chen on 2017/7/7.
 */
public class ListenableChannelFuture<T> extends AbstractCompletableFuture<T> implements GenericFutureListener<Future<T>> {

    protected final Future<T> future;

    public ListenableChannelFuture(Future<T> future) {
        this.future = future;
        this.future.addListener(this);
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        return future.cancel(mayInterruptIfRunning);
    }

    @Override
    public boolean isCancelled() {
        return future.isCancelled();
    }

    @Override
    public boolean isDone() {
        return future.isDone();
    }

    @Override
    public T get() throws InterruptedException, ExecutionException {
        return future.get();
    }

    @Override
    public T get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        return future.get(timeout, unit);
    }

    @Override
    public void operationComplete(Future<T> future) throws Exception {
        FutureListener<T> r = this.listener;
        if (r != null) r.onComplete(this);
    }
}

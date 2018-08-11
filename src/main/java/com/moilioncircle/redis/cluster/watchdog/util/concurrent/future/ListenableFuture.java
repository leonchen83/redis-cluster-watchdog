/*
 * Copyright 2016-2018 Leon Chen
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.moilioncircle.redis.cluster.watchdog.util.concurrent.future;

import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Leon Chen
 * @since 1.0.0
 */
@SuppressWarnings("unchecked")
public class ListenableFuture<T> extends AbstractCompletableFuture<T> {

    /**
     * NEW -> COMPLETING -> NORMAL
     * NEW -> COMPLETING -> EXCEPTIONAL
     * NEW -> CANCELED
     */
    private static final int NEW = 0;
    private static final int COMPLETING = 1;
    private static final int NORMAL = 2;
    private static final int EXCEPTIONAL = 3;
    private static final int CANCELED = 4;
    protected final CountDownLatch latch = new CountDownLatch(1);
    protected final AtomicInteger status = new AtomicInteger(NEW);
    protected volatile Object object;

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        boolean rs;
        if (rs = this.status.compareAndSet(NEW, CANCELED)) latch.countDown();
        return rs;
    }

    @Override
    public boolean isCancelled() {
        return this.status.get() == CANCELED;
    }

    @Override
    public boolean isDone() {
        return this.status.get() > COMPLETING;
    }

    /**
     * @return T value
     * @throws InterruptedException  link to {@link Future#get}
     * @throws ExecutionException    link to {@link Future#get}
     * @throws CancellationException link to {@link Future#get}
     */
    @Override
    public T get() throws InterruptedException, ExecutionException {
        if (status.get() <= COMPLETING) latch.await();
        if (isCancelled()) throw new CancellationException();
        if (object instanceof ExecutionException) {
            throw (ExecutionException) object;
        } else if (object instanceof Throwable) {
            throw new ExecutionException((Throwable) object);
        }
        return (T) object;
    }

    /**
     * @return T value
     * @throws InterruptedException  link to {@link Future#get(long, TimeUnit)}
     * @throws ExecutionException    link to {@link Future#get(long, TimeUnit)}
     * @throws TimeoutException      link to {@link Future#get(long, TimeUnit)}
     * @throws CancellationException link to {@link Future#get(long, TimeUnit)}
     */
    @Override
    public T get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        if (status.get() <= COMPLETING && !latch.await(timeout, unit) && status.get() <= COMPLETING)
            throw new TimeoutException();
        if (isCancelled()) throw new CancellationException();
        if (object instanceof ExecutionException) {
            throw (ExecutionException) object;
        } else if (object instanceof Throwable) {
            throw new ExecutionException((Throwable) object);
        }
        return (T) object;
    }

    /**
     * @param value failure cause
     * @throws IllegalStateException duplicate invoke failure method
     */
    @Override
    public boolean success(T value) {
        if (!this.status.compareAndSet(NEW, COMPLETING)) return false;
        this.object = value;
        this.status.set(NORMAL);
        latch.countDown();
        listener.onComplete(this);
        return true;
    }

    /**
     * @param cause failure cause
     */
    @Override
    public boolean failure(Throwable cause) {
        if (!this.status.compareAndSet(NEW, COMPLETING)) return false;
        this.object = cause;
        this.status.set(EXCEPTIONAL);
        latch.countDown();
        listener.onComplete(this);
        return true;
    }
}

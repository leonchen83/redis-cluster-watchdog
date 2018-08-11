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

package com.moilioncircle.redis.cluster.watchdog.util.concurrent.executor;

import com.moilioncircle.redis.cluster.watchdog.util.concurrent.future.ListenableRunnableFuture;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @author Leon Chen
 * @since 1.0.0
 */
public class ListenableThreadPoolExecutor extends ThreadPoolExecutor {

    private final List<ExecutorListener> listeners = new CopyOnWriteArrayList<>();

    public ListenableThreadPoolExecutor(int corePoolSize, int maximumPoolSize, long keepAliveTime, TimeUnit unit, BlockingQueue<Runnable> workQueue) {
        super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue);
    }

    public ListenableThreadPoolExecutor(int corePoolSize, int maximumPoolSize, long keepAliveTime, TimeUnit unit, BlockingQueue<Runnable> workQueue, ThreadFactory threadFactory) {
        super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, threadFactory);
    }

    public ListenableThreadPoolExecutor(int corePoolSize, int maximumPoolSize, long keepAliveTime, TimeUnit unit, BlockingQueue<Runnable> workQueue, RejectedExecutionHandler handler) {
        super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, handler);
    }

    public ListenableThreadPoolExecutor(int corePoolSize, int maximumPoolSize, long keepAliveTime, TimeUnit unit, BlockingQueue<Runnable> workQueue, ThreadFactory threadFactory, RejectedExecutionHandler handler) {
        super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, threadFactory, handler);
    }

    public void addExecutorListener(ExecutorListener listener) {
        this.listeners.add(listener);
    }

    public void removeExecutorListener(ExecutorListener listener) {
        this.listeners.remove(listener);
    }

    @Override
    protected <T> RunnableFuture<T> newTaskFor(Runnable runnable, T value) {
        return new ListenableRunnableFuture<>(runnable, value);
    }

    @Override
    protected <T> RunnableFuture<T> newTaskFor(Callable<T> callable) {
        return new ListenableRunnableFuture<>(callable);
    }

    @Override
    public ListenableRunnableFuture<?> submit(Runnable task) {
        return (ListenableRunnableFuture<?>) super.submit(task);
    }

    @Override
    public <T> ListenableRunnableFuture<T> submit(Runnable task, T result) {
        return (ListenableRunnableFuture<T>) super.submit(task, result);
    }

    @Override
    public <T> ListenableRunnableFuture<T> submit(Callable<T> task) {
        return (ListenableRunnableFuture<T>) super.submit(task);
    }

    @Override
    protected void beforeExecute(Thread t, Runnable r) {
        super.beforeExecute(t, r);
        for (ExecutorListener listener : listeners) {
            listener.beforeExecute(this, (ListenableRunnableFuture<?>) r);
        }
    }

    @Override
    protected void afterExecute(Runnable r, Throwable t) {
        super.afterExecute(r, t);
        for (ExecutorListener listener : listeners) {
            listener.afterExecute(this, (ListenableRunnableFuture<?>) r, t);
        }
    }

    @Override
    protected void terminated() {
        super.terminated();
        for (ExecutorListener listener : listeners) {
            listener.onTerminated(this);
        }
    }
}

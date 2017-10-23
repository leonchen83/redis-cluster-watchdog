/*
 * Copyright 2016-2017 Leon Chen
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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.FutureTask;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author Leon Chen
 * @since 1.0.0
 */
public class ListenableRunnableFuture<T> extends FutureTask<T> implements CompletableFuture<T> {

    protected static final Log logger = LogFactory.getLog(ListenableRunnableFuture.class);

    protected final AtomicBoolean notifying = new AtomicBoolean(false);
    protected final List<FutureListener<T>> listeners = new CopyOnWriteArrayList<>();

    public ListenableRunnableFuture(Callable<T> callable) {
        super(callable);
    }

    public ListenableRunnableFuture(Runnable runnable, T result) {
        super(runnable, result);
    }

    @Override
    protected void done() {
        notifyListeners();
    }

    @Override
    public boolean addListener(FutureListener<T> listener) {
        boolean rs = listeners.add(listener);
        if (this.isDone()) notifyListeners();
        return rs;
    }

    @Override
    public boolean removeListener(FutureListener<T> listener) {
        return listeners.remove(listener);
    }

    @Override
    public boolean addListeners(List<FutureListener<T>> listeners) {
        boolean rs = this.listeners.addAll(listeners);
        if (this.isDone()) notifyListeners();
        return rs;
    }

    @Override
    public boolean removeListeners(List<FutureListener<T>> listeners) {
        return this.listeners.removeAll(listeners);
    }

    protected void notifyListeners() {
        if (this.listeners.isEmpty()) return;
        if (this.notifying.compareAndSet(false, true)) {
            while (!this.listeners.isEmpty()) {
                for (FutureListener<T> r : this.listeners) {
                    notifyListener(r);
                    this.listeners.remove(r);
                }
            }
            this.notifying.compareAndSet(true, false);
        } else {
            logger.warn("Notifying listener. attempt to ignore this invoking of notifyListeners.");
        }
    }

    protected void notifyListener(FutureListener<T> listener) {
        try {
            listener.onComplete(this);
        } catch (Throwable e) {
            logger.warn("An exception was thrown by " + this.getClass().getName() + ".onComplete()", e);
        }
    }

}

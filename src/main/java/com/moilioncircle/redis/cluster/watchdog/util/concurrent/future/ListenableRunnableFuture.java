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

import java.util.concurrent.Callable;
import java.util.concurrent.FutureTask;

/**
 * @author Leon Chen
 * @since 1.0.0
 */
public class ListenableRunnableFuture<T> extends FutureTask<T> implements CompletableFuture<T> {

    protected volatile FutureListener<T> listener;

    public ListenableRunnableFuture(Callable<T> callable) {
        super(callable);
    }

    public ListenableRunnableFuture(Runnable runnable, T result) {
        super(runnable, result);
    }

    @Override
    protected void done() {
        listener.onComplete(this);
    }

    @Override
    public FutureListener<T> setListener(FutureListener<T> listener) {
        FutureListener<T> r = this.listener;
        this.listener = listener;
        if (isDone() && listener != null) listener.onComplete(this);
        return r;
    }

}

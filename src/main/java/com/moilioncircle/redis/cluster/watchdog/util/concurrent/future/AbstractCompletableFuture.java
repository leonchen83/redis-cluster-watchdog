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

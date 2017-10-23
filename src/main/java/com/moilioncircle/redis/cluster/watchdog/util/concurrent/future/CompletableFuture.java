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

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.function.Function;

/**
 * @author Leon Chen
 * @since 1.0.0
 */
public interface CompletableFuture<T> extends Future<T> {

    default boolean isSuccess() {
        if (!isDone()) return false;
        try { get(); return true; }
        catch (ExecutionException e) { return false; }
        catch (InterruptedException e) { Thread.currentThread().interrupt(); return false; }
    }

    default Throwable cause() {
        if (!isDone()) return null;
        try { get(); return null; }
        catch (ExecutionException e) { return e.getCause(); }
        catch (InterruptedException e) { Thread.currentThread().interrupt(); return null; }
    }

    default boolean success(T value) { throw new UnsupportedOperationException(); }

    default boolean failure(Throwable cause) { throw new UnsupportedOperationException(); }

    default <U> CompletableFuture<U> map(Function<T, U> function) {
        CompletableFuture<U> r = new ListenableFuture<>();
        this.addListener(f -> {
            try { r.success(function.apply(f.get())); }
            catch (ExecutionException e) { r.failure(e); }
            catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        });
        return r;
    }

    boolean addListener(FutureListener<T> listener);

    boolean removeListener(FutureListener<T> listener);

    boolean addListeners(List<FutureListener<T>> listeners);

    boolean removeListeners(List<FutureListener<T>> listeners);
}

package com.moilioncircle.redis.cluster.watchdog.util.concurrent.future;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * @author Leon Chen
 * @since 1.0.0
 */
public abstract class AbstractCompletableFuture<T> implements CompletableFuture<T> {

    protected static final Log logger = LogFactory.getLog(AbstractCompletableFuture.class);

    protected final List<FutureListener<T>> listeners = new CopyOnWriteArrayList<>();

    @Override
    public boolean addListener(FutureListener<T> listener) {
        boolean rs = listeners.add(listener);
        if (this.isDone() && !listeners.isEmpty()) {
            for (FutureListener<T> r : listeners) notifyListener(r);
        }
        return rs;
    }

    @Override
    public boolean removeListener(FutureListener<T> listener) {
        return listeners.remove(listener);
    }

    @Override
    public boolean addListeners(List<FutureListener<T>> listeners) {
        boolean rs = this.listeners.addAll(listeners);
        if (this.isDone() && !this.listeners.isEmpty()) {
            for (FutureListener<T> r : this.listeners) notifyListener(r);
        }
        return rs;
    }

    @Override
    public boolean removeListeners(List<FutureListener<T>> listeners) {
        return this.listeners.removeAll(listeners);
    }

    protected void notifyListener(FutureListener<T> listener) {
        try {
            listener.onComplete(this);
        } catch (Throwable e) {
            logger.warn("An exception was thrown by " + this.getClass().getName() + ".onComplete()", e);
        }
    }
}

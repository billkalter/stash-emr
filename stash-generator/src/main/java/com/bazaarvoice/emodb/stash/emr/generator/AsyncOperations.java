package com.bazaarvoice.emodb.stash.emr.generator;

import com.google.common.collect.Sets;
import com.google.common.util.concurrent.AbstractFuture;
import org.apache.spark.api.java.JavaFutureAction;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class AsyncOperations {

    private final static int MAX_BACKOFF = (int) TimeUnit.SECONDS.toMillis(10);
    
    private final Set<WatchedFuture<?>> _futures = Collections.synchronizedSet(Sets.newIdentityHashSet());
    
    private final ScheduledExecutorService _service;

    public AsyncOperations(ScheduledExecutorService service) {
        _service = service;
    }

    public void watch(JavaFutureAction<?> future) {
        watchAndThen(future, null);
    }

    public <T> void watchAndThen(JavaFutureAction<T> future, Op<T> then) {
        WatchedFuture<T> watchedFuture = new WatchedFuture<T>(future, then);
        _futures.add(watchedFuture);
        watchedFuture.scheduleNextCheck();
    }

    public void awaitAll() throws ExecutionException, InterruptedException {
        WatchedFuture<?> future = _futures.stream().findFirst().orElse(null);
        while (future != null) {
            future.get();
            future = _futures.stream().findFirst().orElse(null);
        }
    }

    public interface Op<T> {
        void run(T value);
    }

    private class WatchedFuture<T> extends AbstractFuture<T> implements Runnable {
        private final JavaFutureAction<T> _future;
        private final Op<T> _op;
        private int _backoff = 100;

        WatchedFuture(JavaFutureAction<T> future, @Nullable Op<T> op) {
            _future = future;
            _op = op;
        }

        @Override
        public void run() {
            try {
                if (_future.isDone()) {
                    if (_future.isCancelled()) {
                        if (!isCancelled()) {
                            cancel(false);
                        }
                    } else {
                        T result = _future.get();
                        if (_op != null) {
                            _op.run(result);
                        }
                        set(result);
                        _futures.remove(this);
                    }
                } else {
                    scheduleNextCheck();
                }
            } catch (ExecutionException e) {
                setException(e.getCause());
            } catch (Throwable t) {
                setException(t);
            }
        }

        private void scheduleNextCheck() {
            long backoff = _backoff;
            _backoff = Math.min((int) (backoff * 1.25), MAX_BACKOFF);
            _service.schedule(this, backoff, TimeUnit.MILLISECONDS);
        }

        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            if (!_future.isCancelled()) {
                _future.cancel(mayInterruptIfRunning);
            }
            return super.cancel(mayInterruptIfRunning);
        }
    }
}

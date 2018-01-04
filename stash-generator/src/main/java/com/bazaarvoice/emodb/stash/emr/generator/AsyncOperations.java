package com.bazaarvoice.emodb.stash.emr.generator;

import com.google.common.base.Throwables;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.AbstractFuture;
import org.apache.spark.api.java.JavaFutureAction;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

public class AsyncOperations {

    private final static int MAX_BACKOFF = (int) TimeUnit.SECONDS.toMillis(10);

    private final Set<WatchedFuture<?>> _watchedFutures = Collections.synchronizedSet(Sets.newIdentityHashSet());
    private final Set<WatchedFuture<?>> _failedFutures = Collections.synchronizedSet(Sets.newIdentityHashSet());
    private final Semaphore _semaphore;
    
    private final ScheduledExecutorService _monitorService;
    private final ExecutorService _opService;

    public AsyncOperations(ScheduledExecutorService monitorService, ExecutorService opService, int maxAsyncOperations) {
        _monitorService = monitorService;
        _opService = opService;
        _semaphore = new Semaphore(maxAsyncOperations);
    }

    public void watch(JavaFutureAction<?> future) {
        watchAndThen(future, null);
    }

    public <T> void watchAndThen(final JavaFutureAction<T> future, final Op<T> then) {
        // Avoid more than the maximum number of parallel operations simultaneously.  Block until available.
        try {
            _semaphore.acquire();
        } catch (InterruptedException e) {
            throw Throwables.propagate(e);
        }
        WatchedFuture<T> watchedFuture = new WatchedFuture<T>(future, then);
        _watchedFutures.add(watchedFuture);
        watchedFuture.scheduleNextCheck();
    }

    public void awaitAll() throws ExecutionException, InterruptedException {
        WatchedFuture<?> future = Stream.concat(_failedFutures.stream(), _watchedFutures.stream()).findFirst().orElse(null);
        while (future != null) {
            future.get();
            future =  Stream.concat(_failedFutures.stream(), _watchedFutures.stream()).findFirst().orElse(null);
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
            if (_future.isDone()) {
                _semaphore.release();
                _opService.submit(() -> {
                    try {
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
                            _watchedFutures.remove(this);
                        }
                    } catch (ExecutionException e) {
                        setException(e.getCause());
                    } catch (Throwable t) {
                        setException(t);
                    }
                });
            } else {
                scheduleNextCheck();
            }
        }

        private void scheduleNextCheck() {
            long backoff = _backoff;
            _backoff = Math.min((int) (backoff * 1.25), MAX_BACKOFF);
            _monitorService.schedule(this, backoff, TimeUnit.MILLISECONDS);
        }

        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            if (!_future.isCancelled()) {
                _future.cancel(mayInterruptIfRunning);
            }
            _watchedFutures.remove(this);
            _failedFutures.add(this);
            return super.cancel(mayInterruptIfRunning);
        }

        @Override
        protected boolean setException(Throwable throwable) {
            _watchedFutures.remove(this);
            _failedFutures.add(this);
            return super.setException(throwable);
        }
    }
}

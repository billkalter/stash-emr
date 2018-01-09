package com.bazaarvoice.emodb.stash.emr.generator;

import com.google.common.collect.Sets;
import com.google.common.util.concurrent.AbstractFuture;
import org.apache.spark.api.java.JavaFutureAction;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.ConcurrentModificationException;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

/**
 * This is a useful helper class for watching {@link JavaFutureAction} instances returned by Spark async operations
 * and optionally performing a follow-up operation based on the results.  This is very similar to Guava's
 * {@link com.google.common.util.concurrent.ListenableFuture} except that the future's returned by Spark don't translate
 * to the listenable API.
 *
 * This implementation uses two executor services: one for monitoring futures and a second for executing the post-completion
 * operations.  This is done to prevent any long-running operation from blocking the monitors.
 */
public class AsyncOperations {

    private final static int MAX_BACKOFF = (int) TimeUnit.SECONDS.toMillis(10);

    private final Set<WatchedFuture<?>> _watchedFutures = Collections.synchronizedSet(Sets.newIdentityHashSet());
    private final Set<WatchedFuture<?>> _failedFutures = Collections.synchronizedSet(Sets.newIdentityHashSet());

    private final ScheduledExecutorService _monitorService;
    private final ExecutorService _opService;

    public AsyncOperations(ScheduledExecutorService monitorService, ExecutorService opService) {
        _monitorService = monitorService;
        _opService = opService;
    }

    public void watch(JavaFutureAction<?> future) {
        watchAndThen(future, null);
    }

    public <T> void watchAndThen(final JavaFutureAction<T> future, final Op<T> then) {
        WatchedFuture<T> watchedFuture = new WatchedFuture<T>(future, then);
        _watchedFutures.add(watchedFuture);
        watchedFuture.scheduleNextCheck();
    }

    public void awaitAll() throws ExecutionException, InterruptedException {
        boolean doneWaiting = false;
        while (!doneWaiting) {
            WatchedFuture<?> future = null;
            try {
                future = Stream.concat(_failedFutures.stream(), _watchedFutures.stream()).findFirst().orElse(null);
                if (future == null) {
                    doneWaiting = true;
                }
            } catch (ConcurrentModificationException e) {
                // Ok, try again
            }
            if (future != null) {
                future.get();
            }
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
            _failedFutures.add(this);
            _watchedFutures.remove(this);
            return super.cancel(mayInterruptIfRunning);
        }

        @Override
        protected boolean setException(Throwable throwable) {
            _failedFutures.add(this);
            _watchedFutures.remove(this);
            return super.setException(throwable);
        }
    }
}

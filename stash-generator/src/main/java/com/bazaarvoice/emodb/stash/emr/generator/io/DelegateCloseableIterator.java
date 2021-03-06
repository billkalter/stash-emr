package com.bazaarvoice.emodb.stash.emr.generator.io;

import com.google.common.base.Throwables;
import com.google.common.collect.AbstractIterator;

import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;

/**
 * Simple implementation of {@link CloseableIterator} which delegates to a provided iterator and closeable instance.
 * The iterator is closed automatically once it is fully iterated.  Otherwise the caller must explicitly close it.
 */
public class DelegateCloseableIterator<T> extends AbstractIterator<T> implements CloseableIterator<T> {

    private final Iterator<T> _delegate;
    private final Closeable _closeable;
    private boolean _closed;

    public DelegateCloseableIterator(Iterator<T> delegate, Closeable closeable) {
        _delegate = delegate;
        _closeable = closeable;
    }

    @Override
    protected T computeNext() {
        if (_delegate.hasNext()) {
            return _delegate.next();
        }
        try {
            close();
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
        return endOfData();
    }

    @Override
    public void close() throws IOException {
        if (!_closed) {
            _closeable.close();
            _closed = true;
        }
    }

    @Override
    protected void finalize() throws Throwable {
        super.finalize();
        close();
    }
}

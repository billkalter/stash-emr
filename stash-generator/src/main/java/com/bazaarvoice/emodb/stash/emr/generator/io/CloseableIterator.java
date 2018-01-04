package com.bazaarvoice.emodb.stash.emr.generator.io;

import java.io.Closeable;
import java.util.Iterator;

public interface CloseableIterator<T> extends Iterator<T>, Closeable {
}

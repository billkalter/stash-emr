package com.bazaarvoice.emodb.stash.emr.generator.io;

import com.google.common.base.Throwables;
import com.google.common.collect.Queues;

import java.lang.ref.WeakReference;
import java.nio.ByteBuffer;
import java.util.concurrent.BlockingQueue;

public class BufferPool {

    private final BlockingQueue<WeakReference<ByteBuffer>> _queue;
    private final int _bufferSize;

    public BufferPool(int poolSize, int bufferSize) {
        _queue = Queues.newArrayBlockingQueue(poolSize);
        for (int i=0; i < poolSize; i++) {
            _queue.offer(new WeakReference<>(null));
        }
        _bufferSize = bufferSize;
    }

    public ByteBuffer getBuffer() {
        try {
            ByteBuffer buffer = null;
            WeakReference<ByteBuffer> reference = _queue.take();
            if (reference != null) {
                buffer = reference.get();
            }
            if (buffer == null) {
                buffer = ByteBuffer.allocateDirect(_bufferSize);
            }
            return buffer;
        } catch (InterruptedException e) {
            throw Throwables.propagate(e);
        }
    }

    public void returnBuffer(ByteBuffer byteBuffer) {
        byteBuffer.position(0);
        _queue.offer(new WeakReference<>(byteBuffer));
    }
}

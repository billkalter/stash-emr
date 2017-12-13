package com.bazaarvoice.emodb.stash.emr.generator.io;

import com.google.common.base.Throwables;
import com.google.common.collect.Queues;

import java.nio.ByteBuffer;
import java.util.concurrent.BlockingQueue;

public class BufferPool {

    private final BlockingQueue<LazyByteBuffer> _queue;
    private final int _bufferSize;

    public BufferPool(int poolSize, int bufferSize) {
        _queue = Queues.newArrayBlockingQueue(poolSize);
        for (int i=0; i < poolSize; i++) {
            _queue.offer(new LazyByteBuffer());
        }
        _bufferSize = bufferSize;
    }

    public ByteBuffer getBuffer() {
        try {
            return _queue.take().getByteBuffer();
        } catch (InterruptedException e) {
            throw Throwables.propagate(e);
        }
    }

    public void returnBuffer(ByteBuffer byteBuffer) {
        _queue.offer(new LazyByteBuffer(byteBuffer));
    }

    private class LazyByteBuffer {
        private ByteBuffer _byteBuffer;

        public LazyByteBuffer() {
        }

        public LazyByteBuffer(ByteBuffer byteBuffer) {
            byteBuffer.position(0);
            _byteBuffer = byteBuffer;
        }

        public ByteBuffer getByteBuffer() {
            if (_byteBuffer == null) {
                _byteBuffer = ByteBuffer.allocateDirect(_bufferSize);
            }
            return _byteBuffer;
        }
    }
}

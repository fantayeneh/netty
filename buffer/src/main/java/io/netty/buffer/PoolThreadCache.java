/*
 * Copyright 2012 The Netty Project
 *
 * The Netty Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package io.netty.buffer;

import java.nio.ByteBuffer;

final class PoolThreadCache {

    final PoolArena<byte[]> heapArena;
    final PoolArena<ByteBuffer> directArena;

    // TODO: Lazy init to minimize overhead
    private final PoolChunkCache<byte[]> smallHeapCache;
    private final PoolChunkCache<ByteBuffer> smallDirectCache;

    // TODO: Test if adding padding helps under contention
    //private long pad0, pad1, pad2, pad3, pad4, pad5, pad6, pad7;

    PoolThreadCache(PoolArena<byte[]> heapArena, PoolArena<ByteBuffer> directArena) {
        this.heapArena = heapArena;
        this.directArena = directArena;
        if (heapArena != null) {
            smallHeapCache = new SubPagePoolChunkCache<byte[]>(128);
        } else {
            smallHeapCache = null;
        }
        if (directArena != null) {
            smallDirectCache = new SubPagePoolChunkCache<ByteBuffer>(128);
        } else {
            smallDirectCache = null;
        }
    }


    final static class SubPagePoolChunkCache<T> extends PoolChunkCache<T> {
        SubPagePoolChunkCache(int size) {
            super(size);
        }

        @Override
        protected void initBuf(
                PoolChunk<T> chunk, long handle, PooledByteBuf<T> buf, int reqCapacity, int normCapacity) {
            chunk.initBufWithSubpage(buf, handle, reqCapacity);
        }
    }

    final static class NormalPoolChunkCache<T> extends PoolChunkCache<T> {
        NormalPoolChunkCache(int size) {
            super(size);
        }

        @Override
        protected void initBuf(
                PoolChunk<T> chunk, long handle, PooledByteBuf<T> buf, int reqCapacity, int normCapacity) {
            chunk.initBuf(buf, handle, reqCapacity);
        }
    }

    abstract static class PoolChunkCache<T> {
        private final Entry<T>[] entries;
        int head;
        int tail;

        @SuppressWarnings("unchecked")
        PoolChunkCache(int size) {
            entries = new Entry[size];
            for (int i = 0; i < entries.length; i++) {
                entries[i] = new Entry<T>();
            }
        }

        protected abstract void initBuf(PoolChunk<T> chunk, long handle,
                                        PooledByteBuf<T> buf, int reqCapacity, int normCapacity);

        /**
         * Add to cache if not already full.
         */
        public boolean add(PoolChunk<T> chunk, long handle) {
            Entry<T> entry = entries[tail];
            if (entry.chunk != null) {
                // cache is full
                return false;
            }
            entry.chunk = chunk;
            entry.handle = handle;
            tail = nextIdx(tail);
            return true;
        }

        /**
         * Allocate something out of the cache if possible
         */
        public boolean allocate(PooledByteBuf<T> buf, int reqCapacity, int normCapacity) {
            Entry<T> entry = entries[head];
            if (entry.chunk == null) {
                return false;
            }
            initBuf(entry.chunk, entry.handle, buf, reqCapacity, normCapacity);
            entry.chunk = null;
            entry.handle = -1;
            head = nextIdx(head);
            return true;
        }

        private int nextIdx(int index) {
            return (index + 1) % entries.length;
        }

        static final class Entry<T> {
            PoolChunk<T> chunk;
            long handle;
        }
    }
}

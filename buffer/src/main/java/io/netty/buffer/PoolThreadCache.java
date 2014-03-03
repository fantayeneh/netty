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
    private final PoolChunkCache<byte[]> tinySubPageHeapCache;
    private final PoolChunkCache<byte[]> smallSubPageHeapCache;
    private final PoolChunkCache<ByteBuffer> tinySubPageDirectCache;
    private final PoolChunkCache<ByteBuffer> smallSubPageDirectCache;

    // TODO: Test if adding padding helps under contention
    //private long pad0, pad1, pad2, pad3, pad4, pad5, pad6, pad7;

    PoolThreadCache(PoolArena<byte[]> heapArena, PoolArena<ByteBuffer> directArena) {
        this.heapArena = heapArena;
        this.directArena = directArena;
        if (heapArena != null) {
            tinySubPageHeapCache = new SubPagePoolChunkCache<byte[]>(128);
            smallSubPageHeapCache = new SubPagePoolChunkCache<byte[]>(128);
        } else {
            tinySubPageHeapCache = null;
            smallSubPageHeapCache = null;
        }
        if (directArena != null) {
            tinySubPageDirectCache = new SubPagePoolChunkCache<ByteBuffer>(128);
            smallSubPageDirectCache = new SubPagePoolChunkCache<ByteBuffer>(128);
        } else {
            tinySubPageDirectCache = null;
            smallSubPageDirectCache = null;
        }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    public boolean allocateTinySubPage(PoolArena area, PooledByteBuf buf, int reqCapacity, int normCapacity) {
        if (area == directArena) {
            if (tinySubPageDirectCache == null) {
                return false;
            }
            return tinySubPageDirectCache.allocate(buf, reqCapacity, normCapacity);
        }
        if (area == heapArena) {
            if (tinySubPageHeapCache == null) {
                return false;
            }
            return tinySubPageHeapCache.allocate(buf, reqCapacity, normCapacity);
        }
        return false;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    public boolean allocateSmallSubPage(PoolArena area, PooledByteBuf buf, int reqCapacity, int normCapacity) {
        if (area == directArena) {
            if (smallSubPageDirectCache == null) {
                return false;
            }
            return smallSubPageDirectCache.allocate(buf, reqCapacity, normCapacity);
        }
        if (area == heapArena) {
            if (smallSubPageHeapCache == null) {
                return false;
            }
            return smallSubPageHeapCache.allocate(buf, reqCapacity, normCapacity);
        }
        return false;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    public boolean cacheTinySubPage(PoolArena area, PoolChunk chunk, long handle) {
        if (area == directArena) {
            if (tinySubPageDirectCache == null) {
                return false;
            }
            return tinySubPageDirectCache.add(chunk, handle);
        }
        if (area == heapArena) {
            if (tinySubPageHeapCache == null) {
                return false;
            }
            return tinySubPageHeapCache.add(chunk, handle);
        }
        return false;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    public boolean cacheSmallSubPage(PoolArena area, PoolChunk chunk, long handle) {
        if (area == directArena) {
            if (smallSubPageDirectCache == null) {
                return false;
            }
            return smallSubPageDirectCache.add(chunk, handle);
        }
        if (area == heapArena) {
            if (smallSubPageHeapCache == null) {
                return false;
            }
            return smallSubPageHeapCache.add(chunk, handle);
        }
        return false;
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
        private int head;
        private int tail;

        @SuppressWarnings("unchecked")
        PoolChunkCache(int size) {
            if ((size & -size) != size) {
                // check if power of two as this is needed for bitwise operations
                throw new IllegalArgumentException("size must be power of two");
            }

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
            // use bitwise operation as this is faster as using modulo.
            return (index + 1) & entries.length -1;
        }

        static final class Entry<T> {
            PoolChunk<T> chunk;
            long handle;
        }
    }
}

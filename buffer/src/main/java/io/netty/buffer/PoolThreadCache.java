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

    private final int tinyCacheSize;
    private final int smallCacheSize;

    // These are lazy initialized when something is added to the cache to minimize memory overhead for Threads that
    // only allocate buffers but never free them. As free is most of the times done by the EventLoop itself.
    //
    // At the moment we only cache stuff which was allocated out of PoolSubpage's but we may also want to cache
    // bigger allocations at least a few of them. Maybe up to 32kb like the jemalloc blog by facebook describes.
    // See https://www.facebook.com/notes/facebook-engineering/scalable-memory-allocation-using-jemalloc/480222803919
    private PoolChunkCache<byte[]> tinySubPageHeapCache;
    private PoolChunkCache<byte[]> smallSubPageHeapCache;
    private PoolChunkCache<ByteBuffer> tinySubPageDirectCache;
    private PoolChunkCache<ByteBuffer> smallSubPageDirectCache;

    // TODO: Test if adding padding helps under contention
    //private long pad0, pad1, pad2, pad3, pad4, pad5, pad6, pad7;

    PoolThreadCache(PoolArena<byte[]> heapArena, PoolArena<ByteBuffer> directArena,
                    int tinyCacheSize, int smallCacheSize) {
        this.heapArena = heapArena;
        this.directArena = directArena;
        this.tinyCacheSize = tinyCacheSize;
        this.smallCacheSize = smallCacheSize;
    }

    /**
     * Try to allocate a tiny buffer out of the cache. Returns {@code true} if successful {@code false} otherwise
     */
    @SuppressWarnings({ "unchecked", "rawtypes " })
    boolean allocateTiny(PoolArena area, PooledByteBuf buf, int reqCapacity) {
        PoolChunkCache cache = cacheForTiny(area, false);
        if (cache == null) {
            // no cache found so just return false here
            return false;
        }
        return cache.allocate(buf, reqCapacity);
    }

    /**
     * Try to allocate a small buffer out of the cache. Returns {@code true} if successful {@code false} otherwise
     */
    @SuppressWarnings({ "unchecked", "rawtypes " })
    boolean allocateSmall(PoolArena area, PooledByteBuf buf, int reqCapacity) {
        PoolChunkCache cache = cacheForSmall(area, false);
        if (cache == null) {
            // no cache found so just return false here
            return false;
        }
        return cache.allocate(buf, reqCapacity);
    }

    /**
     * Add {@link io.netty.buffer.PoolChunk} and handle to the cache if cache has enough room.
     * Returns {@code true} if it fit into the cache {@code false} otherwise.
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    boolean add(PoolArena area, PoolChunk chunk, long handle, int normCapacity) {
        if ((normCapacity & area.subpageOverflowMask) == 0) { // capacity < pageSize
            PoolChunkCache cache;
            if ((normCapacity & 0xFFFFFE00) == 0) { // < 512
                cache = cacheForTiny(area, true);
            } else {
                cache = cacheForSmall(area, true);
            }
            if (cache == null) {
                return false;
            }
            return cache.add(chunk, handle);
        }
        return false;
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    private PoolChunkCache cacheForTiny(PoolArena area, boolean lazyCreate) {
        if (area == directArena) {
            if (tinySubPageDirectCache == null && lazyCreate) {
                tinySubPageDirectCache = new SubPagePoolChunkCache<ByteBuffer>(tinyCacheSize);
            }
            return tinySubPageDirectCache;
        }
        if (area == heapArena) {
            if (tinySubPageHeapCache == null && lazyCreate) {
                tinySubPageHeapCache = new SubPagePoolChunkCache<byte[]>(tinyCacheSize);
            }
            return tinySubPageHeapCache;
        }
        return null;
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    private PoolChunkCache cacheForSmall(PoolArena area, boolean lazyCreate) {
        if (area == directArena) {
            if (smallSubPageDirectCache == null && lazyCreate) {
                smallSubPageDirectCache = new SubPagePoolChunkCache<ByteBuffer>(smallCacheSize);
            }
            return tinySubPageDirectCache;
        }
        if (area == heapArena) {
            if (smallSubPageHeapCache == null && lazyCreate) {
                smallSubPageHeapCache = new SubPagePoolChunkCache<byte[]>(smallCacheSize);
            }
            return smallSubPageHeapCache;
        }
        return null;
    }

    /**
     * Cache used for buffers which are backed by NORMAL size.
     *
     * @param <T>
     */
    static final class SubPagePoolChunkCache<T> extends PoolChunkCache<T> {
        SubPagePoolChunkCache(int size) {
            super(size);
        }

        @Override
        protected void initBuf(
                PoolChunk<T> chunk, long handle, PooledByteBuf<T> buf, int reqCapacity) {
            chunk.initBufWithSubpage(buf, handle, reqCapacity);
        }
    }

    /**
     * Cache used for buffers which are backed by TINY or SMALL size.
     *
     * @param <T>
     */
    static final class NormalPoolChunkCache<T> extends PoolChunkCache<T> {
        NormalPoolChunkCache(int size) {
            super(size);
        }

        @Override
        protected void initBuf(
                PoolChunk<T> chunk, long handle, PooledByteBuf<T> buf, int reqCapacity) {
            chunk.initBuf(buf, handle, reqCapacity);
        }
    }

    /**
     * Cache of {@link PoolChunk} and handles which can be used to allocate a buffer without locking at all.
     *
     * TODO:    Think about if it may make sense to make it a real circular buffer and start to replace the oldest
     *          entries once it is full. This may give less fragmentations but the cost of extra updates of the
     *          entries. But updating the entries should be quite cheap.
     * @param <T>
     */
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

        /**
         * Init the {@link PooledByteBuf} using the provided chunk and handle with the capacity restrictions.
         */
        protected abstract void initBuf(PoolChunk<T> chunk, long handle,
                                        PooledByteBuf<T> buf, int reqCapacity);

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
        public boolean allocate(PooledByteBuf<T> buf, int reqCapacity) {
            Entry<T> entry = entries[head];
            if (entry.chunk == null) {
                return false;
            }
            initBuf(entry.chunk, entry.handle, buf, reqCapacity);
            // only null out the chunk as we only use the chunk to check if the buffer is full or not.
            entry.chunk = null;
            head = nextIdx(head);
            return true;
        }

        private int nextIdx(int index) {
            // use bitwise operation as this is faster as using modulo.
            return (index + 1) & entries.length - 1;
        }

        static final class Entry<T> {
            PoolChunk<T> chunk;
            long handle;
        }
    }
}

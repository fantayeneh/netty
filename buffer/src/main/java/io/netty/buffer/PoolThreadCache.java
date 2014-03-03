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

    // These are lazy initialized when something is added to the cache to minimize memory overhead for Threads that
    // only allocate buffers but never free them. As free is most of the times done by the EventLoop itself.
    private PoolChunkCache<byte[]> tinySubPageHeapCache;
    private PoolChunkCache<byte[]> smallSubPageHeapCache;
    private PoolChunkCache<byte[]> normalHeapCache;
    private PoolChunkCache<ByteBuffer> tinySubPageDirectCache;
    private PoolChunkCache<ByteBuffer> smallSubPageDirectCache;
    private PoolChunkCache<ByteBuffer> normalDirectCache;

    // TODO: Test if adding padding helps under contention
    //private long pad0, pad1, pad2, pad3, pad4, pad5, pad6, pad7;

    PoolThreadCache(PoolArena<byte[]> heapArena, PoolArena<ByteBuffer> directArena) {
        this.heapArena = heapArena;
        this.directArena = directArena;
    }

    /**
     * Different types of caches for the different sizes.
     */
    enum CacheType {
        TINY,
        SMALL,
        NORMAL
    }

    @SuppressWarnings({ "unchecked", "rawtypes " })
    boolean allocate(PoolArena area, CacheType size, PooledByteBuf buf, int reqCapacity) {
        PoolChunkCache cache = cacheForSize(area, size, false);
        if (cache == null) {
            // no cache found so just return false here
            return false;
        }
        return cache.allocate(buf, reqCapacity);
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    boolean cache(PoolArena area, CacheType size, PoolChunk chunk, long handle) {
        PoolChunkCache cache = cacheForSize(area, size, true);
        if (cache == null) {
            // no cache found so just return false here
            return false;
        }
        return cache.add(chunk, handle);
    }

    /**
     * Find the right cache for the given {@link PoolArena} and {@link CacheType}. This returns {@code null} if
     * no cache for the specific type exists at all.
     *
     * TODO: Maybe use different sizes for the caches like bigger ones for TINY and SMALL and smaller cache for  NORMAL.
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    private PoolChunkCache cacheForSize(PoolArena area, CacheType size, boolean lazyCreate) {
        switch (size) {
            case TINY:
                if (area == directArena) {
                    if (tinySubPageDirectCache == null && lazyCreate) {
                        tinySubPageDirectCache = new SubPagePoolChunkCache<ByteBuffer>(128);
                    }
                    return tinySubPageDirectCache;
                }
                if (area == heapArena) {
                    if (tinySubPageHeapCache == null && lazyCreate) {
                        tinySubPageHeapCache = new SubPagePoolChunkCache<byte[]>(128);
                    }
                    return tinySubPageHeapCache;
                }
                return null;
            case SMALL:
                if (area == directArena) {
                    if (smallSubPageDirectCache == null && lazyCreate) {
                        smallSubPageDirectCache = new SubPagePoolChunkCache<ByteBuffer>(128);
                    }
                    return smallSubPageDirectCache;
                }
                if (area == heapArena) {
                    if (smallSubPageHeapCache == null && lazyCreate) {
                        smallSubPageHeapCache = new SubPagePoolChunkCache<byte[]>(128);
                    }
                    return smallSubPageHeapCache;
                }
                return null;
            case NORMAL:
                if (area == directArena) {
                    if (normalDirectCache == null && lazyCreate) {
                        normalDirectCache = new NormalPoolChunkCache<ByteBuffer>(128);
                    }
                    return normalDirectCache;
                }
                if (area == heapArena) {
                    if (normalHeapCache == null && lazyCreate) {
                        normalHeapCache = new NormalPoolChunkCache<byte[]>(128);
                    }
                    return normalHeapCache;
                }
                return null;
            default:
                return null;
        }
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

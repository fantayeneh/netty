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

/**
 * Acts a Thread cache for allocations. This implementation is moduled after
 * <a href="http://people.freebsd.org/~jasone/jemalloc/bsdcan2006/jemalloc.pdf">jemalloc</a> and the descripted
 * technics of <a href="https://www.facebook.com/notes/facebook-engineering/scalable-memory-allocation-using-jemalloc/
 * 480222803919">Scalable memory allocation using jemalloc</a>.
 */
final class PoolThreadCache {
    // 32 kb is the maximum of size which is cached. Similar to what is explained in
    // 'Scalable memory allocation using jemalloc'
    // TODO: Maybe we should make this configurable
    private static final int DEFAULT_MAX_CACHE_SIZE = 32 * 1024;
    // Maximal of 4 different size caches of normal allocations
    // TODO: Maybe we should make this configurable
    private static final int DEFAULT_MAX_CACHE_ARRAY_SIZE = 4;

    final PoolArena<byte[]> heapArena;
    final PoolArena<ByteBuffer> directArena;

    // used for bitshifting when calculate the index of normal caches later
    private final int valNormalDirect;
    private final int valNormalHeap;

    // We cold also create them lazy but this would make the code more clumby and also introduce more branches when
    // check if allocation // adding is possible so not sure it worth it at all.
    private final PoolChunkCache<byte[]> tinySubPageHeapCache;
    private final PoolChunkCache<byte[]> smallSubPageHeapCache;
    private final PoolChunkCache<ByteBuffer> tinySubPageDirectCache;
    private final PoolChunkCache<ByteBuffer> smallSubPageDirectCache;

    // Hold the caches for normal allocations
    // The size of these arrays are maximal 4 and only will hold chunks up to 32kb
    private final PoolChunkCache<byte[]>[] normalHeapCaches;
    private final PoolChunkCache<ByteBuffer>[] normalDirectCaches;

    // TODO: Test if adding padding helps under contention
    //private long pad0, pad1, pad2, pad3, pad4, pad5, pad6, pad7;

    @SuppressWarnings({ "unchecked", "rawtypes " })
    PoolThreadCache(PoolArena<byte[]> heapArena, PoolArena<ByteBuffer> directArena,
                    int tinyCacheSize, int smallCacheSize, int normalCacheSize) {
        this.heapArena = heapArena;
        this.directArena = directArena;
        if (directArena != null) {
            // Create the caches for the direct allocations
            if (tinyCacheSize > 0) {
                tinySubPageDirectCache = new SubPagePoolChunkCache<ByteBuffer>(tinyCacheSize);
            } else {
                tinySubPageDirectCache = null;
            }
            if (smallCacheSize > 0) {
                smallSubPageDirectCache = new SubPagePoolChunkCache<ByteBuffer>(smallCacheSize);
            } else {
                smallSubPageDirectCache = null;
            }

            if (normalCacheSize > 0) {
                valNormalDirect = index(directArena.pageSize);
                int maxCacheSize = Math.min(directArena.chunkSize, DEFAULT_MAX_CACHE_SIZE);
                int arraySize = Math.min(DEFAULT_MAX_CACHE_ARRAY_SIZE, maxCacheSize / directArena.pageSize);
                normalDirectCaches = new PoolChunkCache[arraySize];
                int size = directArena.pageSize;
                for (int i = 0; i < normalDirectCaches.length; i++) {
                    normalDirectCaches[index(size) >> valNormalDirect] =
                            new NormalPoolChunkCache<ByteBuffer>(normalCacheSize);
                    size = directArena.normalizeCapacity(size);
                }
            } else {
                normalDirectCaches = null;
                valNormalDirect = -1;
            }
        } else {
            // No directArea is configured so just null out all caches
            tinySubPageDirectCache = null;
            smallSubPageDirectCache = null;
            normalDirectCaches = null;
            valNormalDirect = -1;
        }
        if (heapArena != null) {
            // Create the caches for the heap allocations
            if (tinyCacheSize > 0) {
                tinySubPageHeapCache = new SubPagePoolChunkCache<byte[]>(tinyCacheSize);
            } else {
                tinySubPageHeapCache = null;
            }
            if (smallCacheSize > 0) {
                smallSubPageHeapCache = new SubPagePoolChunkCache<byte[]>(smallCacheSize);
            } else {
                smallSubPageHeapCache = null;
            }

            if (normalCacheSize > 0) {
                valNormalHeap = index(heapArena.pageSize);
                int maxCacheSize = Math.min(heapArena.chunkSize, DEFAULT_MAX_CACHE_SIZE);
                int arraySize = Math.min(DEFAULT_MAX_CACHE_ARRAY_SIZE, maxCacheSize / heapArena.pageSize);
                normalHeapCaches = new PoolChunkCache[arraySize];
                int size = heapArena.pageSize;
                for (int i = 0; i < normalHeapCaches.length; i++) {
                    normalHeapCaches[index(size) >> valNormalHeap] =
                            new NormalPoolChunkCache<byte[]>(normalCacheSize);
                    size = heapArena.normalizeCapacity(size);
                }
            } else {
                normalHeapCaches = null;
                valNormalHeap = -1;
            }
        } else {
            // No heapArea is configured so just null out all caches
            tinySubPageHeapCache = null;
            smallSubPageHeapCache = null;
            normalHeapCaches = null;
            valNormalHeap = -1;
        }
    }

    // TODO: Find a better name
    private static int index(int val) {
        int res = 0;
        while (val > 1) {
            val >>= 1;
            res++;
        }
        return res;
    }

    /**
     * Try to allocate a tiny buffer out of the cache. Returns {@code true} if successful {@code false} otherwise
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    boolean allocateTiny(PoolArena area, PooledByteBuf buf, int reqCapacity) {
        return allocate(cacheForTiny(area), buf, reqCapacity);
    }

    /**
     * Try to allocate a small buffer out of the cache. Returns {@code true} if successful {@code false} otherwise
     */
    @SuppressWarnings({ "unchecked", "rawtypes " })
    boolean allocateSmall(PoolArena area, PooledByteBuf buf, int reqCapacity) {
        return allocate(cacheForSmall(area), buf, reqCapacity);
    }

    /**
     * Try to allocate a small buffer out of the cache. Returns {@code true} if successful {@code false} otherwise
     */
    @SuppressWarnings({ "unchecked", "rawtypes " })
    boolean allocateNormal(PoolArena area, PooledByteBuf buf, int reqCapacity, int normCapacity) {
        return allocate(cacheForNormal(area, normCapacity), buf, reqCapacity);
    }

    @SuppressWarnings({ "unchecked", "rawtypes " })
    private static boolean allocate(PoolChunkCache cache, PooledByteBuf buf, int reqCapacity) {
        if (cache == null) {
            // no cache found so just return false here
            return false;
        }
        return cache.allocate(buf, reqCapacity);
    }

    /**
     * Add {@link PoolChunk} and {@code handle} to the cache if there is enough room.
     * Returns {@code true} if it fit into the cache {@code false} otherwise.
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    boolean add(PoolArena area, PoolChunk chunk, long handle, int normCapacity) {
        PoolChunkCache cache;
        if ((normCapacity & area.subpageOverflowMask) == 0) { // capacity < pageSize
            if ((normCapacity & 0xFFFFFE00) == 0) { // < 512
                cache = cacheForTiny(area);
            } else {
                cache = cacheForSmall(area);
            }
        } else {
            cache = cacheForNormal(area, normCapacity);
        }
        if (cache == null) {
            return false;
        }
        return cache.add(chunk, handle);
    }

    /**
     *  Should be called if the Thread that uses this cache is about to exist to release resources out of the cache
     */
    void free() {
        free(tinySubPageDirectCache);
        free(smallSubPageDirectCache);
        free(normalDirectCaches);
        free(tinySubPageHeapCache);
        free(smallSubPageHeapCache);
        free(normalHeapCaches);
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    private static void free(PoolChunkCache[] caches) {
        if (caches == null) {
            return;
        }
        for (int i = 0; i < caches.length; i++) {
            free(caches[i]);
        }
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    private static void free(PoolChunkCache cache) {
        if (cache == null) {
            return;
        }
        cache.clear();
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    private PoolChunkCache cacheForTiny(PoolArena area) {
        if (area == directArena) {
            return tinySubPageDirectCache;
        }
        if (area == heapArena) {
            return tinySubPageHeapCache;
        }
        throw new IllegalStateException("unkown area: " + area);
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    private PoolChunkCache cacheForSmall(PoolArena area) {
        if (area == directArena) {
            return smallSubPageDirectCache;
        }
        if (area == heapArena) {
            return smallSubPageHeapCache;
        }
        throw new IllegalStateException("unkown area: " + area);
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    private PoolChunkCache cacheForNormal(PoolArena area, int normCapacity) {
        if (area == directArena) {
            if (normalDirectCaches == null) {
                return null;
            }
            int idx = index(normCapacity >> valNormalDirect);
            if (idx > normalDirectCaches.length - 1) {
                return null;
            }
            return normalDirectCaches[idx];
        }
        if (area == heapArena) {
            if (normalHeapCaches == null) {
                return null;
            }
            int idx = index(normCapacity >> valNormalHeap);
            if (idx > normalHeapCaches.length - 1) {
                return null;
            }
            return normalHeapCaches[idx];
        }
        throw new IllegalStateException("unkown area: " + area);
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

        /**
         * Clear out this cache and free up all previous cached {@link PoolChunk}s and {@code handle}s.
         */
        public void clear() {
            for (int i = tail;; i = nextIdx(i)) {
                Entry<T> entry = entries[i];
                PoolChunk<T> chunk = entry.chunk;
                if (chunk == null) {
                    // all cleared
                    break;
                }
                // need to synchronize on the area from which it was allocated before.
                synchronized (chunk.arena) {
                    chunk.parent.free(chunk, entry.handle);
                }
                entry.chunk = null;
            }
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

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

    private final PoolChunkCache<byte[]> smallHeapCache;
    private final PoolChunkCache<ByteBuffer> smallDirectCache;

    // TODO: Test if adding padding helps under contention
    //private long pad0, pad1, pad2, pad3, pad4, pad5, pad6, pad7;

    PoolThreadCache(PoolArena<byte[]> heapArena, PoolArena<ByteBuffer> directArena) {
        this.heapArena = heapArena;
        this.directArena = directArena;
        if (heapArena != null) {
            smallHeapCache = new PoolChunkCache<byte[]>();
        } else {
            smallHeapCache = null;
        }
        if (directArena != null) {
            smallDirectCache = new PoolChunkCache<ByteBuffer>();
        } else {
            smallDirectCache = null;
        }
    }

    final static class PoolChunkCache<T> {
        private Entry<T> first;
        private Entry<T> last;

        public void offer(PoolChunk<T> chunk, long handle) {
            Entry<T> entry = new Entry<T>(chunk, handle);
            if (first == null) {
                first = last = entry;
            } else {
                last.next = entry;
            }
        }

        public Entry<T> poll() {
            Entry<T> entry = first;
            if (entry == null) {
                return null;
            }
            first = entry.next;
            if (first == null) {
                last = null;
            }
            return entry;
        }

        static final class Entry<T> {
            private Entry<T> next;
            final PoolChunk<T> chunk;
            final long handle;
            Entry(PoolChunk<T> chunk, long handle) {
                this.chunk = chunk;
                this.handle = handle;
            }
        }
    }
}

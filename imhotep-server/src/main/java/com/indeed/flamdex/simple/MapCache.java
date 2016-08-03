/*
 * Copyright (C) 2014 Indeed Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
 package com.indeed.flamdex.simple;

import com.google.common.base.Joiner;
import com.indeed.util.mmap.DirectMemory;
import com.indeed.util.mmap.MMapBuffer;

import org.apache.log4j.Logger;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

/**
 * @author jplaisance
 */
public final class MapCache {
    private static final Logger log = Logger.getLogger(MapCache.class);
    private static final MapCache instance = new MapCache();

    public static Pool getPool() {
        return new Pool(instance);
    }


    private static final class CountedRef {
        final Path path;
        final MMapBuffer buffer;
        int count;

        private CountedRef(final Path p, final MMapBuffer buf) {
            this.path = p;
            this.buffer = buf;
            this.count = 0;
        }

        public synchronized void incrementRefCount() {
            this.count++;
        }

        public synchronized int decrementRefCount() {
            assert count > 0;
            this.count--;
            return this.count;
        }
    }

    private final Map<Path, CountedRef> mappingCache;

    private MapCache() {
        mappingCache = new HashMap<>();
    }

    private synchronized CountedRef getOrOpen(final Path path) throws IOException {
        CountedRef ref = mappingCache.get(path);
        if (ref == null) {
            ref = new CountedRef(path, new MMapBuffer(path, FileChannel.MapMode.READ_ONLY, ByteOrder.LITTLE_ENDIAN));
            mappingCache.put(path, ref);
        }
        ref.incrementRefCount();
        return ref;
    }

    private synchronized void release(final CountedRef ref) throws IOException {
        if (ref.decrementRefCount() == 0) {
            ref.buffer.close();
            mappingCache.remove(ref.path);
        }
    }

    /*
     * Pool of mappings used by a given use case. The underlying mappings are ref-counted and are shared among Pools.
     */
    public static class Pool implements AutoCloseable {
        private final MapCache cache;
        private final Map<Path, CountedRef> usedBuffers;
        private final Throwable creationStackTrace;
        private boolean closed = false;

        private Pool(final MapCache cache) {
            this.cache = cache;
            this.usedBuffers = new HashMap<>();
            this.creationStackTrace = new Throwable();
        }

        private synchronized MMapBuffer getBuffer(final Path path) throws IOException {
            CountedRef ref = usedBuffers.get(path);
            if (ref == null) {
                ref = cache.getOrOpen(path);
                usedBuffers.put(path, ref);
            }
            return ref.buffer;
        }

        public DirectMemory getDirectMemory(final Path path) throws IOException {
            return getBuffer(path).memory();
        }

        public long[] getMappedAddressAndLen(final String path) throws IOException, URISyntaxException {
            return getMappedAddressAndLen(Paths.get(new URI(path)));
        }

        public long[] getMappedAddressAndLen(final Path path) throws IOException {
            final DirectMemory memory = getDirectMemory(path);
            return new long[]{memory.getAddress(), memory.length()};
        }

        @Override
        public synchronized void close() throws IOException {
            if (closed) {
                return;
            }

            for (final CountedRef ref : usedBuffers.values()) {
                cache.release(ref);
            }

            closed = true;
        }

        @Override
        protected synchronized void finalize() throws Throwable {
            if (!closed) {
                log.error("MapPool was not closed properly! Stack trace at creation: \n"
                                  + Joiner.on("\n").join(creationStackTrace.getStackTrace()));
            }
            close();
            super.finalize();
        }
    }
}

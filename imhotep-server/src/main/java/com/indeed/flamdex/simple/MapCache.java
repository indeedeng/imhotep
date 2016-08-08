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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * @author jplaisance
 */
public final class MapCache {
    private static final Logger log = Logger.getLogger(MapCache.class);
    private static MapCache cacheSingleton;

    private static MapCache getCache() {
        if (MapCache.cacheSingleton == null) {
            synchronized (MapCache.class) {
                if (MapCache.cacheSingleton == null) {
                    MapCache.cacheSingleton = new MapCache();
                }
            }
        }
        return MapCache.cacheSingleton;
    }

    public static Pool getPool() {
        final MapCache cache = getCache();
        return new Pool(cache);
    }


    private static final class CountedRef {
        final Path path;
        final MMapBuffer buffer;
        int count;

        private CountedRef(Path p, MMapBuffer buf) {
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
        this.mappingCache = new HashMap<>();
    }

    private synchronized CountedRef internalGet(Path path) throws IOException {
        CountedRef ref = mappingCache.get(path);
        if (ref == null) {
            final MMapBuffer mmapBuf;

            mmapBuf = new MMapBuffer(path, FileChannel.MapMode.READ_ONLY, ByteOrder.LITTLE_ENDIAN);
            ref = new CountedRef(path, mmapBuf);
            mappingCache.put(path, ref);
        }
        ref.incrementRefCount();
        return ref;
    }

    private synchronized void internalRemove(CountedRef ref) throws IOException {
        ref.buffer.close();
        this.mappingCache.remove(ref.path);
    }


    /*
     * Pool class used to access mmappings
     */
    public static class Pool implements AutoCloseable {
        private final MapCache cache;
        private final ArrayList<CountedRef> trackingList;
        private final String creationStackTrace;
        private boolean closed = false;

        private Pool(MapCache cache) {
            this.cache = cache;
            this.trackingList = new ArrayList<>(40);
            this.creationStackTrace = Arrays.toString((new Throwable()).getStackTrace());
        }

        public DirectMemory getDirectMemory(Path path) throws IOException {
            final CountedRef ref;

            ref = cache.internalGet(path);
            track(ref);
            return ref.buffer.memory();
        }

        public long[] getMappedAddressAndLen(String path) throws IOException, URISyntaxException {
            final long[] results =  getMappedAddressAndLen(Paths.get(new URI(path)));
            return results;
        }

        public long[] getMappedAddressAndLen(Path path) throws IOException {
            final CountedRef ref;

            ref = cache.internalGet(path);
            track(ref);
            final DirectMemory memory = ref.buffer.memory();
            final long[] results = new long[]{memory.getAddress(), memory.length()};
            return results;
        }

        private synchronized void track(CountedRef ref) {
            this.trackingList.add(ref);
        }

        @Override
        public synchronized void close() throws IOException {
            if (closed) {
                return;
            }

            for (CountedRef ref : trackingList) {
                final int newRefCount = ref.decrementRefCount();
                if (newRefCount == 0) {
                    try {
                        cache.internalRemove(ref);
                    } catch(IOException e) {
                        e.printStackTrace();
                        throw e;
                    }
                }
            }

            closed = true;
        }

        @Override
        protected void finalize() throws Throwable {
            if (! closed) {
                log.error("MapPool was not closed properly! Stack trace at creation: \n"
                                  + this.creationStackTrace);
            }
            this.close();
            super.finalize();
        }
    }
}

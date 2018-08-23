/*
 * Copyright (C) 2018 Indeed Inc.
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

package com.indeed.imhotep.service;

import com.google.common.base.Throwables;
import com.indeed.imhotep.MemoryReservationContext;
import com.indeed.imhotep.MemoryReserver;
import com.indeed.util.core.Pair;
import com.indeed.util.core.io.Closeables2;
import com.indeed.util.core.reference.SharedReference;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class ConcurrentFlamdexReaderFactory {
    private static final Logger log = Logger.getLogger(ConcurrentFlamdexReaderFactory.class);

    private final MemoryReserver memory;
    private final FlamdexReaderSource factory;
    private final ExecutorService threadPool = Executors.newCachedThreadPool();


    //    private final LoadingCache<Pair<Path, Integer>, SharedReference<CachedFlamdexReader>> flamdexReaderLoadingCache;


    public ConcurrentFlamdexReaderFactory(final MemoryReserver memory, final FlamdexReaderSource factory) {
        this.memory = memory;
        this.factory = factory;
    }

    final class CreateReader implements Callable<Void> {
        final Path path;
        final int numDocs;
        final Map<Path, SharedReference<CachedFlamdexReader>> result;

        public CreateReader(Path path, int numDocs, Map<Path, SharedReference<CachedFlamdexReader>> result) {
            this.path = path;
            this.numDocs = numDocs;
            this.result = result;
        }

        public Void call() {
            final SharedReference<CachedFlamdexReader> reader;
            try {
                reader = createFlamdexReader(path, numDocs);
            }
            catch (final Exception ex) {
                log.warn("unable to create reader for: " + path, ex);
                throw Throwables.propagate(ex);
            }
            result.put(path, reader);
            return null;
        }

        private SharedReference<CachedFlamdexReader> createFlamdexReader(Path path, int numDocs) throws IOException {
            if (numDocs <= 0) {
                return SharedReference.create(new CachedFlamdexReader(new MemoryReservationContext(memory), factory.openReader(path)));
            } else {
                return SharedReference.create(new CachedFlamdexReader(new MemoryReservationContext(memory), factory.openReader(path, numDocs)));
            }
        }
    }

    /** For each requested shard, return an id and a CachedFlamdexReaderReference. */
    Map<Path, SharedReference<CachedFlamdexReader>> getFlamdexReaders(List<Pair<Path, Integer>> pathsAndNumDocs)
            throws IOException {

        final ConcurrentHashMap<Path, SharedReference<CachedFlamdexReader>> result = new ConcurrentHashMap<>();

        final List<CreateReader> createReaders = new ArrayList<>();

        for (final Pair<Path, Integer> request : pathsAndNumDocs) {
            createReaders.add(new CreateReader(request.getFirst(), request.getSecond(), result));
        }

        /* Creating readers can actually be a bit expensive, since it can
           involve opening and reading metadata.txt files within shards. Do this
           in parallel, per IMTEPD-188. */
        try {
            final List<Future<Void>> outcomes =
                    threadPool.invokeAll(createReaders, 5, TimeUnit.MINUTES);
            for (final Future<Void> outcome: outcomes) {
                outcome.get();
            }
        }
        catch (final Throwable ex) {
            Closeables2.closeAll(log, result.values());
            throw new IOException("unable to create all requested FlamdexReaders", ex);
        }
        return result;
    }
}

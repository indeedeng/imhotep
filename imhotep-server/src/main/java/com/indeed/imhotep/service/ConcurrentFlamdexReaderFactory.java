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
import com.indeed.imhotep.client.Host;
import com.indeed.imhotep.scheduling.ImhotepTask;
import com.indeed.util.core.io.Closeables2;
import com.indeed.util.core.reference.SharedReference;
import com.indeed.util.core.threads.NamedThreadFactory;
import org.apache.log4j.Logger;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Provides parallel FlamdexReader construction using the provided FlamdexReaderSource.
 */
public class ConcurrentFlamdexReaderFactory {
    private static final Logger log = Logger.getLogger(ConcurrentFlamdexReaderFactory.class);
    // TODO: remove after CPU locking for this is enabled
    private static final int IO_THREAD_COUNT = 28;

    private final MemoryReserver memory;
    private final FlamdexReaderSource factory;
    private final ShardLocator shardLocator;
    private final ThreadPoolExecutor threadPool = new BlockingThreadPoolExecutor(IO_THREAD_COUNT, IO_THREAD_COUNT,
            new NamedThreadFactory("ConcurrentFlamdexReaderFactory", true, log));


    // TODO: re-enable flamdex reader cache after making sure it doesn't lead to leaks
    //    private final LoadingCache<Pair<Path, Integer>, SharedReference<CachedFlamdexReader>> flamdexReaderLoadingCache;

    public ConcurrentFlamdexReaderFactory(
            final MemoryReserver memory,
            final FlamdexReaderSource factory,
            final ShardLocator shardLocator
    ) {
        this.memory = memory;
        this.factory = factory;
        this.shardLocator = shardLocator;

//        flamdexReaderLoadingCache = CacheBuilder.newBuilder().maximumSize(100000).expireAfterAccess(config.getFlamdexReaderCacheMaxDurationMillis(),
//                TimeUnit.MILLISECONDS).removalListener((RemovalListener<Pair<Path, Integer>, SharedReference<CachedFlamdexReader>>) notification ->
//                    Closeables2.closeQuietly(notification.getValue(), log))
//                .build(new CacheLoader<Pair<Path, Integer>, SharedReference<CachedFlamdexReader>>() {
//            @Override
//            public SharedReference<CachedFlamdexReader> load(Pair<Path, Integer> key) throws Exception {
//                return createFlamdexReader(key.getKey(), key.getValue());
//            }
//        });
    }

    public static class CreateRequest {
        public final String dataset;
        public final String shardName;
        @Nullable public final Host shardOwner;
        public final int numDocs;
        public final String userName;
        public final String clientName;

        /**
         * @param dataset    is the directory under the {@code rootDir}.
         * @param shardName  is the shard directory name. See {@link FlamdexInfo}.
         * @param shardOwner is the owner server of shard
         * @param numDocs    is the number of docs for the shard. Should be &lt;= 0 if it is unknown.
         * @param userName
         * @param clientName
         */
        public CreateRequest(
                final String dataset,
                final String shardName,
                @Nullable final Host shardOwner,
                final int numDocs,
                final String userName,
                final String clientName) {
            this.dataset = dataset;
            this.shardName = shardName;
            this.shardOwner = shardOwner;
            this.numDocs = numDocs;
            this.userName = userName;
            this.clientName = clientName;
        }
    }

    private final class CreateReaderTask implements Callable<Void> {
        final Map<Path, SharedReference<CachedFlamdexReader>> result;
        final CreateRequest createRequest;

        CreateReaderTask(CreateRequest createRequest, Map<Path, SharedReference<CachedFlamdexReader>> result) {
            this.result = result;
            this.createRequest = createRequest;
        }

        private Path locateShard(final CreateRequest createRequest) {
            return shardLocator.locateShard(createRequest.dataset, createRequest.shardName, createRequest.shardOwner)
                    .orElseThrow(() -> new IllegalArgumentException("Unable to locate shard for dataset=" + createRequest.dataset + ", shardName=" + createRequest.shardName));
        }

        public Void call() {
            final SharedReference<CachedFlamdexReader> reader;
            ImhotepTask.setup(createRequest.userName, createRequest.clientName, createRequest.dataset, createRequest.shardName, createRequest.numDocs);
            final Path shardPath = locateShard(createRequest);
            try {
            // TODO: enable locking
//            try (final Closeable ignored = TaskScheduler.CPUScheduler.lockSlot()) {
                reader = createFlamdexReader(shardPath, createRequest.numDocs);
            } catch (final Exception ex) {
                log.warn("unable to create reader for: " + shardPath, ex);
                throw Throwables.propagate(ex);
            } finally {
                ImhotepTask.clear();
            }
            result.put(shardPath, reader);
            return null;
        }

        private SharedReference<CachedFlamdexReader> createFlamdexReader(final Path path, final int numDocs) throws IOException {
            if (numDocs <= 0) {
                return SharedReference.create(new CachedFlamdexReader(new MemoryReservationContext(memory), factory.openReader(path)));
            } else {
                return SharedReference.create(new CachedFlamdexReader(new MemoryReservationContext(memory), factory.openReader(path, numDocs)));
            }
        }
    }

    /** For each requested shard, return a path and a CachedFlamdexReader shared reference. */
    public Map<Path, SharedReference<CachedFlamdexReader>> constructFlamdexReaders(final Collection<CreateRequest> createRequests)
            throws IOException {

        final ConcurrentHashMap<Path, SharedReference<CachedFlamdexReader>> result = new ConcurrentHashMap<>();
        final List<CreateReaderTask> createReaderTasks = createRequests.stream().map(
                request -> new CreateReaderTask(request, result)
        ).collect(Collectors.toList());

        /* Creating readers can actually be a bit expensive, since it can
           involve opening and reading metadata.txt files within shards. Do this
           in parallel, per IMTEPD-188. */
        try {
            final List<Future<Void>> outcomes =
                    threadPool.invokeAll(createReaderTasks, 15, TimeUnit.MINUTES);
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

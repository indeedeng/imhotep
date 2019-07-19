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
import com.indeed.flamdex.api.FlamdexReader;
import com.indeed.imhotep.MemoryReservationContext;
import com.indeed.imhotep.MemoryReserver;
import com.indeed.imhotep.SlotTiming;
import com.indeed.imhotep.scheduling.ImhotepTask;
import com.indeed.util.core.io.Closeables2;
import com.indeed.util.core.reference.SharedReference;
import com.indeed.util.core.threads.NamedThreadFactory;
import lombok.Data;
import org.apache.log4j.Logger;

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
        public final ShardHostInfo shardHostInfo;
        public final int numDocs;
        public final String userName;
        public final String clientName;
        public final byte priority;

        /**
         * @param dataset    is the directory under the {@code rootDir}.
         * @param shardHostInfo is the information about shardName(shard directory name, see {@link FlamdexInfo}), shardServer and shardOwner.
         * @param numDocs    is the number of docs for the shard. Should be &lt;= 0 if it is unknown.
         * @param userName
         * @param clientName
         */
        public CreateRequest(
                final String dataset,
                final ShardHostInfo shardHostInfo,
                final int numDocs,
                final String userName,
                final String clientName,
                final byte priority) {
            this.dataset = dataset;
            this.shardHostInfo = shardHostInfo;
            this.numDocs = numDocs;
            this.userName = userName;
            this.clientName = clientName;
            this.priority = priority;
        }
    }

    private final class CreateReaderTask implements Callable<SlotTiming> {
        final Map<Path, SharedReference<CachedFlamdexReader>> result;
        final CreateRequest createRequest;

        CreateReaderTask(CreateRequest createRequest, Map<Path, SharedReference<CachedFlamdexReader>> result) {
            this.result = result;
            this.createRequest = createRequest;
        }

        private Path locateShard(final CreateRequest createRequest) {
            return shardLocator.locateShard(createRequest.dataset, createRequest.shardHostInfo)
                    .orElseThrow(() -> new IllegalArgumentException("Unable to locate shard for dataset=" + createRequest.dataset + ", " +
                            "shardHostInfo=" + createRequest.shardHostInfo.toString()));
        }

        public SlotTiming call() {
            final SlotTiming slotTiming = new SlotTiming();
            final SharedReference<CachedFlamdexReader> reader;
            ImhotepTask.setup(createRequest.userName, createRequest.clientName, createRequest.priority, createRequest.dataset,
                    createRequest.shardHostInfo.getShardName(), createRequest.numDocs, slotTiming);
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
            return slotTiming;
        }

        private SharedReference<CachedFlamdexReader> createFlamdexReader(final Path path, final int numDocs) throws IOException {
            final FlamdexReader flamdexReader = (numDocs <= 0) ? factory.openReader(path) : factory.openReader(path, numDocs);
            return SharedReference.create(new CachedFlamdexReader(new MemoryReservationContext(memory), flamdexReader));
        }
    }

    /** For each requested shard, return a path and a CachedFlamdexReader shared reference. */
    public ConstructFlamdexReadersResult constructFlamdexReaders(final Collection<CreateRequest> createRequests)
            throws IOException {

        final ConcurrentHashMap<Path, SharedReference<CachedFlamdexReader>> result = new ConcurrentHashMap<>();
        final SlotTiming slotTiming = new SlotTiming();
        final List<CreateReaderTask> createReaderTasks = createRequests.stream().map(
                request -> new CreateReaderTask(request, result)
        ).collect(Collectors.toList());

        /* Creating readers can actually be a bit expensive, since it can
           involve opening and reading metadata.txt files within shards. Do this
           in parallel, per IMTEPD-188. */
        try {
            final List<Future<SlotTiming>> outcomes =
                    threadPool.invokeAll(createReaderTasks, 15, TimeUnit.MINUTES);
            for (final Future<SlotTiming> outcome: outcomes) {
                slotTiming.addFromSlotTiming(outcome.get());
            }
        }
        catch (final Throwable ex) {
            Closeables2.closeAll(log, result.values());
            throw new IOException("unable to create all requested FlamdexReaders", ex);
        }

        return new ConstructFlamdexReadersResult(result, slotTiming);
    }

    @Data
    static class ConstructFlamdexReadersResult {
        final Map<Path, SharedReference<CachedFlamdexReader>> flamdexes;
        final SlotTiming slotTiming;
    }
}

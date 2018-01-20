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
package com.indeed.imhotep.service;

import com.google.common.base.Function;
import com.indeed.flamdex.api.DocIdStream;
import com.indeed.flamdex.api.FieldsCardinalityMetadata;
import com.indeed.flamdex.api.FlamdexOutOfMemoryException;
import com.indeed.flamdex.api.FlamdexReader;
import com.indeed.flamdex.api.IntTermDocIterator;
import com.indeed.flamdex.api.IntTermIterator;
import com.indeed.flamdex.api.IntValueLookup;
import com.indeed.flamdex.api.StringTermDocIterator;
import com.indeed.flamdex.api.StringTermIterator;
import com.indeed.flamdex.api.StringValueLookup;
import com.indeed.flamdex.lucene.LuceneFlamdexReader;
import com.indeed.flamdex.ramses.RamsesFlamdexWrapper;
import com.indeed.imhotep.ImhotepMemoryCache;
import com.indeed.imhotep.ImhotepStatusDump;
import com.indeed.imhotep.MemoryReservationContext;
import com.indeed.imhotep.MetricKey;
import com.indeed.util.core.Either;
import com.indeed.util.core.io.Closeables2;
import com.indeed.util.core.reference.ReloadableSharedReference;
import org.apache.log4j.Logger;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static com.indeed.util.core.Either.Left;
import static com.indeed.util.core.Either.Right;

/**
 * @author jsgroth
 */
public class CachedFlamdexReader implements FlamdexReader, MetricCache {
    private static final Logger log = Logger.getLogger(CachedFlamdexReader.class);

    @Nullable
    private final MemoryReservationContext memory;

    private final int memoryReservedForIndex;

    private final FlamdexReader wrapped;

    private final MetricCache metricCache;

    private final Map<String, Long> intDocFreqCache = new ConcurrentHashMap<>();
    private final Map<String, Long> stringDocFreqCache = new ConcurrentHashMap<>();

    public CachedFlamdexReader(final MemoryReservationContext memory,
                               final FlamdexReader wrapped,
                               @Nullable final String indexName,
                               @Nullable final String shardName,
                               @Nullable final ImhotepMemoryCache<MetricKey, IntValueLookup> freeCache) {
        //closer will free these in the opposite order that they are added
        this.memory = memory;
        this.wrapped = wrapped;
        if (wrapped instanceof LuceneFlamdexReader || wrapped instanceof RamsesFlamdexWrapper) {
            memoryReservedForIndex = getMemoryUsedForLuceneIndex(wrapped.getDirectory());
            memory.claimMemory(memoryReservedForIndex);
        } else {
            memoryReservedForIndex = 0;
        }
        metricCache = new MetricCacheImpl(
                new Function<String, Either<FlamdexOutOfMemoryException, IntValueLookup>>() {
                    @Override
                    public Either<FlamdexOutOfMemoryException, IntValueLookup> apply(final String metric) {
                        if (freeCache != null) {
                            final IntValueLookup intValueLookup = freeCache.tryRemove(new MetricKey(indexName, shardName, metric));
                            if (intValueLookup != null) {
                                memory.dehoist(intValueLookup.memoryUsed());
                                return Right.of(intValueLookup);
                            }
                        }
                        final long memoryUsed = wrapped.memoryRequired(metric);
                        if (!memory.claimMemory(memoryUsed)) {
                            return Left.of(new FlamdexOutOfMemoryException());
                        }
                        final IntValueLookup lookup;
                        try {
                            lookup = wrapped.getMetric(metric);
                            if (lookup.memoryUsed() != memoryUsed) {
                                log.error("FlamdexReader.memoryUsed(" + metric + "):" + memoryUsed + " does not match lookup.memoryUsed(): " + lookup.memoryUsed());
                                if (memoryUsed > lookup.memoryUsed()) {
                                    memory.releaseMemory(memoryUsed - lookup.memoryUsed());
                                } else {
                                    memory.claimMemory(lookup.memoryUsed() - memoryUsed);
                                }
                            }
                        } catch (final RuntimeException e) {
                            memory.releaseMemory(memoryUsed);
                            throw e;
                        } catch (final FlamdexOutOfMemoryException e) { // not sure why this would be thrown, but just in case...
                            memory.releaseMemory(memoryUsed);
                            return Left.of(e);
                        }
                        return Right.of(lookup);
                    }
                },
                new ReloadableSharedReference.Closer<Map.Entry<String, IntValueLookup>>() {
                    @Override
                    public void close(final Map.Entry<String, IntValueLookup> metric) {
                        if (freeCache == null) {
                            memory.releaseMemory(metric.getValue());
                        } else {
                            freeCache.put(new MetricKey(indexName, shardName, metric.getKey()), metric.getValue());
                            memory.hoist(metric.getValue().memoryUsed());
                        }
                    }
                }
        );
    }

    @Override
    public Collection<String> getIntFields() {
        return wrapped.getIntFields();
    }

    @Override
    public Collection<String> getStringFields() {
        return wrapped.getStringFields();
    }

    @Override
    public int getNumDocs() {
        return wrapped.getNumDocs();
    }

    @Override
    public Path getDirectory() {
        return wrapped.getDirectory();
    }

    @Override
    public DocIdStream getDocIdStream() {
        return wrapped.getDocIdStream();
    }

    @Override
    public IntTermIterator getIntTermIterator(final String field) {
        return wrapped.getIntTermIterator(field);
    }

    @Override
    public IntTermIterator getUnsortedIntTermIterator(final String field) {
        return wrapped.getUnsortedIntTermIterator(field);
    }

    @Override
    public StringTermIterator getStringTermIterator(final String field) {
        return wrapped.getStringTermIterator(field);
    }

    @Override
    public IntTermDocIterator getIntTermDocIterator(final String field) {
        return wrapped.getIntTermDocIterator(field);
    }

    @Override
    public StringTermDocIterator getStringTermDocIterator(final String field) {
        return wrapped.getStringTermDocIterator(field);
    }

    @Override
    public long getIntTotalDocFreq(final String field) {
        Long docFreq = intDocFreqCache.get(field);
        if (docFreq == null) {
            docFreq = wrapped.getIntTotalDocFreq(field);
            intDocFreqCache.put(field, docFreq);
        }
        return docFreq;
    }

    @Override
    public long getStringTotalDocFreq(final String field) {
        Long docFreq = stringDocFreqCache.get(field);
        if (docFreq == null) {
            docFreq = wrapped.getStringTotalDocFreq(field);
            stringDocFreqCache.put(field, docFreq);
        }
        return docFreq;
    }

    @Override
    public Collection<String> getAvailableMetrics() {
        return wrapped.getAvailableMetrics();
    }

    @Override
    public IntValueLookup getMetric(final String metric) throws FlamdexOutOfMemoryException {
        return metricCache.getMetric(metric);
    }

    //string lookups are always mmapped so it's not as big of a deal to not cache the references
    public StringValueLookup getStringLookup(final String field) throws FlamdexOutOfMemoryException {
        return wrapped.getStringLookup(field);
    }

    @Override
    public List<ImhotepStatusDump.MetricDump> getMetricDump() {
        return metricCache.getMetricDump();
    }

    @Override
    public Set<String> getLoadedMetrics() {
        return metricCache.getLoadedMetrics();
    }

    @Override
    public long memoryRequired(final String metric) {
        return wrapped.memoryRequired(metric);
    }

    @Override
    public FieldsCardinalityMetadata getFieldsMetadata() {
        return wrapped.getFieldsMetadata();
    }

    @Override
    public void close() {
        try {
            Closeables2.closeAll(log, metricCache, wrapped);
        } finally {
            if (memory == null) {
                return;
            }
            if (memoryReservedForIndex > 0) {
                memory.releaseMemory(memoryReservedForIndex);
            }
            if (memory.usedMemory() > 0) {
                log.error("CachedFlamdexReader is leaking! memory reserved after all memory has been freed: " + memory.usedMemory());
            }
            Closeables2.closeQuietly(memory, log);
        }
    }

    public FlamdexReader getWrapped() {
        return this.wrapped;
    }

    private static int getMemoryUsedForLuceneIndex(final Path shardDirectory) {
        if (!Files.exists(shardDirectory)) {
            return 0;
        }

        int memoryNeeded = 0;
        try (DirectoryStream<Path> files = Files.newDirectoryStream(shardDirectory, new DirectoryStream.Filter<Path>() {
            @Override
            public boolean accept(final Path entry) throws IOException {
                return entry.getFileName().toString().endsWith(".tii");
            }
        })) {
            for (final Path tii : files) {
                memoryNeeded += 4 * Files.size(tii); // reserve 4 times the index file size to account for decompression
            }
        } catch (final IOException e) {
            throw new IllegalStateException("Could not get memory used for lucene index", e);
        }

        return memoryNeeded;
    }
}

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
import com.indeed.imhotep.ImhotepStatusDump;
import com.indeed.imhotep.MemoryReservationContext;
import com.indeed.imhotep.MemoryReserver;
import com.indeed.util.core.io.Closeables2;
import com.indeed.util.core.reference.ReloadableSharedReference;
import it.unimi.dsi.fastutil.ints.IntIterator;
import org.apache.log4j.Logger;

import javax.annotation.Nullable;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.Map;
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

    private final FlamdexReader wrapped;

    private final MetricCache metricCache;

    private final Map<String, Long> intDocFreqCache = new ConcurrentHashMap<>();
    private final Map<String, Long> stringDocFreqCache = new ConcurrentHashMap<>();

    public CachedFlamdexReader(final MemoryReservationContext memory,
                               final FlamdexReader wrapped) {
        //closer will free these in the opposite order that they are added
        this.memory = memory;
        this.wrapped = wrapped;
        metricCache = new MetricCacheImpl(
                metric -> {
                    final long memoryUsed = wrapped.memoryRequired(metric);
                    final MemoryReserver.AllocationResult allocationResult = memory.claimMemory(memoryUsed);
                    if (allocationResult != MemoryReserver.AllocationResult.ALLOCATED) {
                        return Left.of(new FlamdexOutOfMemoryException(allocationResult.msg));
                    }
                    final IntValueLookup lookup;
                    try {
                        lookup = wrapped.getMetric(metric);
                        if (lookup.memoryUsed() != memoryUsed) {
                            log.error("FlamdexReader.memoryUsed(" + metric + "):" + memoryUsed + " does not match lookup.memoryUsed(): " + lookup.memoryUsed());
                            if (memoryUsed > lookup.memoryUsed()) {
                                memory.releaseMemory(memoryUsed - lookup.memoryUsed());
                            } else {
                                final MemoryReserver.AllocationResult allocationResult1 = memory.claimMemory(lookup.memoryUsed() - memoryUsed);
                                if (allocationResult1 != MemoryReserver.AllocationResult.ALLOCATED) {
                                    throw new FlamdexOutOfMemoryException(allocationResult1.msg);
                                }
                            }
                        }
                    } catch (final RuntimeException e) {
                        memory.releaseMemory(memoryUsed);
                        throw e;
                    } catch (final FlamdexOutOfMemoryException e) {
                        memory.releaseMemory(memoryUsed);
                        return Left.of(e);
                    }
                    return Right.of(lookup);
                },
                new ReloadableSharedReference.Closer<Map.Entry<String, IntValueLookup>>() {
                    @Override
                    public void close(final Map.Entry<String, IntValueLookup> metric) {
                        memory.releaseMemory(metric.getValue());
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
    public long memoryRequired(final String metric) {
        return wrapped.memoryRequired(metric);
    }

    @Override
    public FieldsCardinalityMetadata getFieldsMetadata() {
        return wrapped.getFieldsMetadata();
    }

    @Override
    public IntIterator getDeletedDocIterator() {
        return wrapped.getDeletedDocIterator();
    }

    @Override
    public void close() {
        try {
            Closeables2.closeAll(log, metricCache, wrapped);
        } finally {
            if (memory == null) {
                return;
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
}

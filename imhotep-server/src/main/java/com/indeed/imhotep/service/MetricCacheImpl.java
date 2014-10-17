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
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Collections2;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.indeed.util.core.Either;
import com.indeed.util.core.io.Closeables2;
import com.indeed.util.core.reference.ReloadableSharedReference;
import com.indeed.util.core.reference.SharedReference;
import com.indeed.flamdex.api.FlamdexOutOfMemoryException;
import com.indeed.flamdex.api.IntValueLookup;
import com.indeed.imhotep.ImhotepStatusDump;
import org.apache.log4j.Logger;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author jplaisance
 */
public final class MetricCacheImpl implements MetricCache {

    private static final Logger log = Logger.getLogger(MetricCacheImpl.class);

    private final LoadingCache<String, ReloadableSharedReference<IntValueLookup, FlamdexOutOfMemoryException>> reloaders;

    private final Map<String, IntValueLookup> loadedMetrics;

    private final ReloadableSharedReference.Closer<Map.Entry<String, IntValueLookup>> closeMetric;

    private boolean closed = false;

    public MetricCacheImpl(final Function<String, Either<FlamdexOutOfMemoryException, IntValueLookup>> loadMetric, final ReloadableSharedReference.Closer<Map.Entry<String, IntValueLookup>> closeMetric) {
        this.closeMetric = closeMetric;
        loadedMetrics = Maps.newHashMap();
        this.reloaders = CacheBuilder.newBuilder().build(
                new CacheLoader<String, ReloadableSharedReference<IntValueLookup, FlamdexOutOfMemoryException>>() {
                    @Override
                    public ReloadableSharedReference<IntValueLookup, FlamdexOutOfMemoryException> load(@Nullable final String metric) {
                        return ReloadableSharedReference.create(
                                new ReloadableSharedReference.Loader<IntValueLookup, FlamdexOutOfMemoryException>() {
                                    @Override
                                    public IntValueLookup load() throws FlamdexOutOfMemoryException {
                                        final IntValueLookup lookup = loadMetric.apply(metric).get();
                                        synchronized (loadedMetrics) {
                                            if (closed) {
                                                closeMetric.close(Maps.immutableEntry(metric, lookup));
                                                throw new IllegalStateException("already closed");
                                            }
                                            if (loadedMetrics.containsKey(metric)) {
                                                closeMetric.close(Maps.immutableEntry(metric, lookup));
                                                log.error("metric "+metric+" is already loaded");
                                                return loadedMetrics.get(metric);
                                            }
                                            loadedMetrics.put(metric, lookup);
                                        }
                                        return lookup;
                                    }
                                },
                                new ReloadableSharedReference.Closer<IntValueLookup>() {
                                    @Override
                                    public void close(final IntValueLookup intValueLookup) {
                                        synchronized (loadedMetrics) {
                                            if (!closed) {
                                                final IntValueLookup removed = loadedMetrics.remove(metric);
                                                if (intValueLookup != removed) {
                                                    log.error("something is wrong with ReloadableSharedReference, load and close should be called in pairs");
                                                }
                                                closeMetric.close(Maps.immutableEntry(metric, intValueLookup));
                                            }
                                        }
                                    }
                                }
                        );
                    }
                }
        );
    }

    @Override
    public IntValueLookup getMetric(final String metric) throws FlamdexOutOfMemoryException {
        return new CachedIntValueLookup(reloaders.getUnchecked(metric).copy());
    }

    @Override
    public List<ImhotepStatusDump.MetricDump> getMetricDump() {
        final List<ImhotepStatusDump.MetricDump> ret = Lists.newArrayList();
        synchronized (loadedMetrics) {
            if (closed) {
                throw new IllegalStateException("already closed");
            }
            for (final Map.Entry<String, IntValueLookup> entry : loadedMetrics.entrySet()) {
                ret.add(new ImhotepStatusDump.MetricDump(entry.getKey(), entry.getValue().memoryUsed()));
            }
        }
        return ret;
    }

    @Override
    public Set<String> getLoadedMetrics() {
        synchronized (loadedMetrics) {
            if (closed) {
                throw new IllegalStateException("already closed");
            }
            return Sets.newHashSet(loadedMetrics.keySet());
        }
    }

    private static final class CachedIntValueLookup implements IntValueLookup {
        private final SharedReference<IntValueLookup> reference;
        private final IntValueLookup metric;

        private CachedIntValueLookup(final SharedReference<IntValueLookup> reference) {
            this.reference = reference;
            metric = reference.get();
        }

        @Override
        public long getMin() {
            return metric.getMin();
        }

        @Override
        public long getMax() {
            return metric.getMax();
        }

        @Override
        public void lookup(final int[] docIds, final long[] values, final int n) {
            metric.lookup(docIds, values, n);
        }

        @Override
        public long memoryUsed() {
            return metric.memoryUsed();
        }

        @Override
        public void close() {
            Closeables2.closeQuietly(reference, log);
        }
    }

    @Override
    public void close() {
        synchronized (loadedMetrics) {
            if (!closed) {
                closed = true;
                try {
                    Closeables2.closeAll(log, Collections2.transform(loadedMetrics.entrySet(), new Function<Map.Entry<String, IntValueLookup>, Closeable>() {
                        public Closeable apply(final Map.Entry<String, IntValueLookup> metric) {
                            return closeMetric.asCloseable(metric);
                        }
                    }));
                } finally {
                    loadedMetrics.clear();
                }
            }
        }
    }
}

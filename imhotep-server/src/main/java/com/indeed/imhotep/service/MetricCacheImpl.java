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
import com.google.common.collect.Collections2;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.indeed.flamdex.api.FlamdexOutOfMemoryException;
import com.indeed.flamdex.api.IntValueLookup;
import com.indeed.imhotep.ImhotepStatusDump;
import com.indeed.util.core.Either;
import com.indeed.util.core.io.Closeables2;
import com.indeed.util.core.reference.ReloadableSharedReference;
import com.indeed.util.core.reference.SharedReference;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;

/**
 * @author jplaisance
 */
public final class MetricCacheImpl implements MetricCache {

    private static final Logger log = Logger.getLogger(MetricCacheImpl.class);

    private final ConcurrentMap<String, SharedReference<IntValueLookup>> metrics = new ConcurrentHashMap<>();

    private final Function<String, Either<FlamdexOutOfMemoryException, IntValueLookup>> loadMetric;
    private final ReloadableSharedReference.Closer<Map.Entry<String, IntValueLookup>> closeMetric;

    private boolean closed = false;

    public MetricCacheImpl(final Function<String, Either<FlamdexOutOfMemoryException, IntValueLookup>> loadMetric,
                           final ReloadableSharedReference.Closer<Map.Entry<String, IntValueLookup>> closeMetric) {
        this.loadMetric = loadMetric;
        this.closeMetric = closeMetric;
    }

    @Override
    public IntValueLookup getMetric(final String metric) throws FlamdexOutOfMemoryException {
        if (closed) {
            throw new IllegalStateException("already closed");
        }
        final SharedReference<IntValueLookup> metricRef = metrics.get(metric);
        if (metricRef != null) {
            SharedReference<IntValueLookup> copy = metricRef.tryCopy();
            if (copy != null) {
                return new CachedIntValueLookup(copy);
            }
        }
        final IntValueLookup intValueLookup = loadMetric.apply(metric).get();
        final SharedReference<IntValueLookup> ref = SharedReference.create(intValueLookup, () -> closeMetric.close(Maps.immutableEntry(metric, intValueLookup)));
        while (true) {
            final SharedReference<IntValueLookup> oldRef = metrics.putIfAbsent(metric, ref);
            if (oldRef != null) {
                final SharedReference<IntValueLookup> copy = oldRef.tryCopy();
                if (copy != null) {
                    Closeables2.closeQuietly(ref, log);
                    return new CachedIntValueLookup(copy);
                }
                if (!metrics.replace(metric, oldRef, ref)) {
                    continue;
                }
            }
            if (!Config.isCloseMetricsWhenUnused()) {
                ref.copy(); // Leak here to close metric only on close session
            }
            return new CachedIntValueLookup(ref);
        }
    }

    @Override
    public List<ImhotepStatusDump.MetricDump> getMetricDump() {
        final List<ImhotepStatusDump.MetricDump> ret = Lists.newArrayList();
        if (!closed) {
            for (final Map.Entry<String, SharedReference<IntValueLookup>> entry : metrics.entrySet()) {
                try (final SharedReference<IntValueLookup> copy = entry.getValue().tryCopy()) {
                    if (copy != null) {
                        ret.add(new ImhotepStatusDump.MetricDump(entry.getKey(), copy.get().memoryUsed()));
                    }
                } catch (IOException e) {
                    throw Throwables.propagate(e);
                }
            }
        }
        return ret;
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
        if (!closed) {
            closed = true;
            Closeables2.closeAll(log, Collections2.transform(metrics.entrySet(),
                    (metric) -> () -> {
                        try (final SharedReference<IntValueLookup> copy = metric.getValue().tryCopy()) {
                            if (copy != null) {
                                if (Config.isCloseMetricsWhenUnused()) {
                                    log.error("Metric '" + metric.getKey() + "' has leaked in MetricCacheImpl");
                                }
                                closeMetric.close(Maps.immutableEntry(metric.getKey(), copy.get()));
                            }
                        }
                    }
            ));
        }
    }

    public static final class Config {
        private static boolean closeMetricsWhenUnused = "true".equalsIgnoreCase(System.getProperty("com.indeed.imhotep.service.MetricCacheImpl.closeUnused"));

        public static boolean isCloseMetricsWhenUnused() {
            return closeMetricsWhenUnused;
        }

        public static void setCloseMetricsWhenUnused(final boolean closeMetricsWhenUnused) {
            Config.closeMetricsWhenUnused = closeMetricsWhenUnused;
        }
    }
}

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

import com.google.common.collect.Lists;
import com.google.common.primitives.Longs;
import com.indeed.flamdex.api.FlamdexOutOfMemoryException;
import com.indeed.flamdex.api.FlamdexReader;
import com.indeed.flamdex.api.IntValueLookup;
import com.indeed.flamdex.reader.MockFlamdexReader;
import com.indeed.imhotep.ImhotepMemoryPool;
import com.indeed.imhotep.MemoryReservationContext;
import com.indeed.imhotep.MemoryReserver;
import com.indeed.util.core.reference.AtomicSharedReference;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * @author jsgroth
 */
public class TestCachedFlamdexReader {
    private static boolean CLOSE_METRICS_WHEN_UNUSED;

    @BeforeClass
    public static void setMetricCacheImplConfig() {
        CLOSE_METRICS_WHEN_UNUSED = MetricCacheImpl.Config.isCloseMetricsWhenUnused();
        MetricCacheImpl.Config.setCloseMetricsWhenUnused(true);
    }

    @AfterClass
    public static void resetMetricCacheImplConfig() {
        MetricCacheImpl.Config.setCloseMetricsWhenUnused(CLOSE_METRICS_WHEN_UNUSED);
    }

    @Test
    public void testClosing() throws FlamdexOutOfMemoryException, IOException {
        final AtomicBoolean closed = new AtomicBoolean(false);
        FlamdexReader r = new SillyFlamdexReader() {
            @Override
            public void close() throws IOException {
                closed.set(true);
            }
        };
        final MemoryReserver memory = new ImhotepMemoryPool(Long.MAX_VALUE);
        final AtomicSharedReference<CachedFlamdexReader> resource = AtomicSharedReference.create(new CachedFlamdexReader(new MemoryReservationContext(memory), r));
        assertFalse(closed.get());
        try (final CachedFlamdexReaderReference ignored = new CachedFlamdexReaderReference(resource.getCopy())) {
            assertFalse(closed.get());
        }
        assertFalse(closed.get());
        for (int i = 2; i < 10; ++i) {
            final List<CachedFlamdexReaderReference> l = Lists.newArrayList();
            for (int j = 0; j < i; ++j) {
                l.add(new CachedFlamdexReaderReference(resource.getCopy()));
                assertFalse(closed.get());
            }
            for (final CachedFlamdexReaderReference cfr : l) {
                cfr.close();
                assertFalse(closed.get());
            }
        }
        final CachedFlamdexReaderReference cfr3 = new CachedFlamdexReaderReference(resource.getCopy());
        assertFalse(closed.get());
        resource.unset();
        assertFalse(closed.get());
        cfr3.close();
        assertTrue(closed.get());

        closed.set(false);
        r = new SillyFlamdexReader() {
            @Override
            public void close() throws IOException {
                closed.set(true);
            }
        };
        resource.set(new CachedFlamdexReader(new MemoryReservationContext(memory), r));
        try (final CachedFlamdexReaderReference ignored = new CachedFlamdexReaderReference(resource.getCopy())) {
            assertFalse(closed.get());
        }
        assertFalse(closed.get());
        resource.set(null);
        assertTrue(closed.get());
    }

    @Test
    public void testLookupCaching() throws FlamdexOutOfMemoryException, IOException {
        try (final FlamdexReader r = new SillyFlamdexReader();
             final MemoryReserver memory = new ImhotepMemoryPool(Long.MAX_VALUE);
             final CachedFlamdexReader cfr = new CachedFlamdexReader(new MemoryReservationContext(memory), r)) {
            final IntValueLookup l1 = cfr.getMetric("m1");
            final long memoryUsed = memory.usedMemory();
            try (final IntValueLookup l2 = cfr.getMetric("m1")) {
                assertEquals(memoryUsed, memory.usedMemory());
            }
            final IntValueLookup l3 = cfr.getMetric("m1");
            assertEquals(memoryUsed, memory.usedMemory());
            l1.close();
            assertEquals(memoryUsed, memory.usedMemory());
            l3.close();
            assertEquals(0, memory.usedMemory());
            try (final IntValueLookup l4 = cfr.getMetric("m1")) {
                assertEquals(memoryUsed, memory.usedMemory());
            }
            assertEquals(0, memory.usedMemory());
        }
    }

    @Test
    public void testMemory1() throws FlamdexOutOfMemoryException, IOException {
        try (final FlamdexReader r = new SillyFlamdexReader();
             final MemoryReserver memory = new ImhotepMemoryPool(7L);
             final CachedFlamdexReader cfr = new CachedFlamdexReader(new MemoryReservationContext(memory), r)) {
            final IntValueLookup[] lookups = new IntValueLookup[5];
            for (int i = 0; i < lookups.length; ++i) {
                lookups[i] = cfr.getMetric("m1");
            }
            assertEquals(5L, memory.usedMemory());
            for (int i = 0; i < 4; ++i) {
                lookups[i].close();
                assertEquals(5L, memory.usedMemory());
            }
            lookups[4].close();
            assertEquals(0L, memory.usedMemory());
        }
    }

    @Test
    public void testMemory2() throws FlamdexOutOfMemoryException, IOException {
        try (final FlamdexReader r = new SillyFlamdexReader();
             final MemoryReserver memory = new ImhotepMemoryPool(7L)) {
            final AtomicSharedReference<CachedFlamdexReader> resource = AtomicSharedReference.create(new CachedFlamdexReader(new MemoryReservationContext(memory), r));
            try (
                    final CachedFlamdexReaderReference cfr = new CachedFlamdexReaderReference(resource.getCopy());
                    final CachedFlamdexReaderReference cfr1 = new CachedFlamdexReaderReference(resource.getCopy());
                    final CachedFlamdexReaderReference cfr2 = new CachedFlamdexReaderReference(resource.getCopy())
            ) {
                final IntValueLookup l1 = cfr1.getMetric("m1");
                assertEquals(5L, memory.usedMemory());
                final IntValueLookup l2 = cfr2.getMetric("m1");
                assertEquals(5L, memory.usedMemory());
                l1.close();
                assertEquals(5L, memory.usedMemory());
                l2.close();
                assertEquals(0L, memory.usedMemory());
            }
        }
    }

    @Test
    public void testMemory3() throws FlamdexOutOfMemoryException, IOException {
        try (final FlamdexReader r = new SillyFlamdexReader();
             final MemoryReserver memory = new ImhotepMemoryPool(6L);
             final CachedFlamdexReader cfr = new CachedFlamdexReader(new MemoryReservationContext(memory), r)) {
            cfr.getMetric("m1");
            assertEquals(5L, memory.usedMemory());
            cfr.getMetric("m1");
            assertEquals(5L, memory.usedMemory());
            try {
                cfr.getMetric("m2");
                // error if previous line doesn't throw FOOME
                fail();
            } catch (final FlamdexOutOfMemoryException e) {
                assertEquals(5L, memory.usedMemory());
            }
        }
    }

    @Test
    public void testLookupWrap() throws FlamdexOutOfMemoryException {
        final FlamdexReader r = new SillyFlamdexReader();
        final CachedFlamdexReader cfr = new CachedFlamdexReader(new MemoryReservationContext(new ImhotepMemoryPool(Long.MAX_VALUE)), r);
        final IntValueLookup l = cfr.getMetric("m1");
        final int[] docIds = {0, 2, 4, 9000, Integer.MIN_VALUE};
        final long[] values = new long[docIds.length];
        l.lookup(docIds, values, 3);
        assertEquals(Arrays.asList(1L, 9L, 25L, 0L, 0L), Longs.asList(values));
    }

    @Test
    public void testMemoryUsedIsntUsed() throws FlamdexOutOfMemoryException, IOException {
        try (final FlamdexReader r = new SillyFlamdexReader();
             final MemoryReserver memory = new ImhotepMemoryPool(10L);
             final CachedFlamdexReader cfr = new CachedFlamdexReader(new MemoryReservationContext(memory), r)) {
            final IntValueLookup l = cfr.getMetric("‽");
            assertEquals(10L, memory.usedMemory());
            final IntValueLookup l2 = cfr.getMetric("‽");
            l.close();
            assertEquals(10L, memory.usedMemory());
            l2.close();
            assertEquals(0L, memory.usedMemory());
        }
    }

    private static class SillyFlamdexReader extends MockFlamdexReader {
        private SillyFlamdexReader() {
            super(Collections.singletonList("m1"), Collections.<String>emptyList(), Collections.singletonList("m1"), 5);
            super.addIntTerm("m1", 1, Collections.singletonList(0));
            super.addIntTerm("m1", 4, Collections.singletonList(1));
            super.addIntTerm("m1", 9, Collections.singletonList(2));
            super.addIntTerm("m1", 16, Collections.singletonList(3));
            super.addIntTerm("m1", 25, Collections.singletonList(4));
        }

        @Override
        public IntValueLookup getMetric(final String metric) throws FlamdexOutOfMemoryException {
            if ("‽".equals(metric)) {
                return new IntValueLookup() {
                    @Override
                    public long getMin() {
                        return 0;
                    }

                    @Override
                    public long getMax() {
                        return 0;
                    }

                    @Override
                    public void lookup(final int[] docIds, final long[] values, final int n) {
                    }

                    @Override
                    public long memoryUsed() {
                        return 10L;
                    }

                    @Override
                    public void close() {
                    }
                };
            }

            return new IntValueLookup() {
                final int[] lookup = {1, 4, 9, 16, 25};

                @Override
                public long getMin() {
                    return 1;
                }

                @Override
                public long getMax() {
                    return 25;
                }

                @Override
                public void lookup(final int[] docIds, final long[] values, final int n) {
                    for (int i = 0; i < n; ++i) {
                        values[i] = lookup[docIds[i]];
                    }
                }

                @Override
                public long memoryUsed() {
                    return 5L;
                }

                @Override
                public void close() {
                }
            };
        }

        @Override
        public long memoryRequired(final String metric) {
            if ("‽".equals(metric)) {
                return 10L;
            }
            return 5L;
        }
    }
}

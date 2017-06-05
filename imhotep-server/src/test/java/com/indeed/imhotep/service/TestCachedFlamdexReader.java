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

import com.google.common.collect.Lists;
import com.google.common.primitives.Longs;
import com.indeed.flamdex.api.FlamdexOutOfMemoryException;
import com.indeed.flamdex.api.FlamdexReader;
import com.indeed.flamdex.api.IntValueLookup;
import com.indeed.flamdex.reader.MockFlamdexReader;
import com.indeed.imhotep.CachedMemoryReserver;
import com.indeed.imhotep.ImhotepMemoryCache;
import com.indeed.imhotep.ImhotepMemoryPool;
import com.indeed.imhotep.MemoryReservationContext;
import com.indeed.imhotep.MemoryReserver;
import com.indeed.imhotep.MetricKey;
import com.indeed.util.core.reference.AtomicSharedReference;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * @author jsgroth
 */
public class TestCachedFlamdexReader {

    @Test
    public void testClosing() throws FlamdexOutOfMemoryException, IOException {
        final ImhotepMemoryCache<MetricKey, IntValueLookup> cache = new ImhotepMemoryCache<MetricKey, IntValueLookup>();
        final AtomicBoolean closed = new AtomicBoolean(false);
        FlamdexReader r = new SillyFlamdexReader() {
            @Override
            public void close() throws IOException {
                closed.set(true);
            }
        };
        final MemoryReserver memory = new CachedMemoryReserver(new ImhotepMemoryPool(Long.MAX_VALUE), cache);
        final AtomicSharedReference<CachedFlamdexReader> resource = AtomicSharedReference.create(new CachedFlamdexReader(new MemoryReservationContext(memory), r, "test", "test", cache));
        assertFalse(closed.get());
        CachedFlamdexReaderReference cfr2 = new CachedFlamdexReaderReference(resource.getCopy());
        assertFalse(closed.get());
        cfr2.close();
        assertFalse(closed.get());
        for (int i = 2; i < 10; ++i) {
            List<CachedFlamdexReaderReference> l = Lists.newArrayList();
            for (int j = 0; j < i; ++j) {
                l.add(new CachedFlamdexReaderReference(resource.getCopy()));
                assertFalse(closed.get());
            }
            for (CachedFlamdexReaderReference cfr : l) {
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
        resource.set(new CachedFlamdexReader(new MemoryReservationContext(memory), r, "test", "test", cache));
        cfr2 = new CachedFlamdexReaderReference(resource.getCopy());
        assertFalse(closed.get());
        cfr2.close();
        assertFalse(closed.get());
        resource.set(null);
        assertTrue(closed.get());
    }

    @Test
    public void testLookupCaching() throws FlamdexOutOfMemoryException {
        final ImhotepMemoryCache<MetricKey, IntValueLookup> cache = new ImhotepMemoryCache<MetricKey, IntValueLookup>();
        FlamdexReader r = new SillyFlamdexReader();
        final MemoryReserver memory = new CachedMemoryReserver(new ImhotepMemoryPool(Long.MAX_VALUE), cache);
        CachedFlamdexReader
                cfr = new CachedFlamdexReader(new MemoryReservationContext(memory), r, "test", "test", cache);
        IntValueLookup l1 = cfr.getMetric("m1");
        long memoryUsed = memory.usedMemory();
        IntValueLookup l2 = cfr.getMetric("m1");
        assertEquals(memoryUsed, memory.usedMemory());
        l2.close();
        IntValueLookup l3 = cfr.getMetric("m1");
        assertEquals(memoryUsed, memory.usedMemory());
        l1.close();
        assertEquals(memoryUsed, memory.usedMemory());
        l3.close();
        assertEquals(0, memory.usedMemory());
        IntValueLookup l4 = cfr.getMetric("m1");
        assertEquals(memoryUsed, memory.usedMemory());
        l4.close();
        assertEquals(0, memory.usedMemory());
    }

    @Test
    public void testMemory1() throws FlamdexOutOfMemoryException {
        final ImhotepMemoryCache<MetricKey, IntValueLookup> cache = new ImhotepMemoryCache<MetricKey, IntValueLookup>();
        FlamdexReader r = new SillyFlamdexReader();
        MemoryReserver memory = new CachedMemoryReserver(new ImhotepMemoryPool(7L), cache);
        CachedFlamdexReader cfr = new CachedFlamdexReader(new MemoryReservationContext(memory), r, "test", "test", cache);
        IntValueLookup[] lookups = new IntValueLookup[5];
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

    @Test
    public void testMemory2() throws FlamdexOutOfMemoryException {
        final ImhotepMemoryCache<MetricKey, IntValueLookup> cache = new ImhotepMemoryCache<MetricKey, IntValueLookup>();
        FlamdexReader r = new SillyFlamdexReader();
        MemoryReserver memory = new CachedMemoryReserver(new ImhotepMemoryPool(7L), cache);
        AtomicSharedReference<CachedFlamdexReader> resource = AtomicSharedReference.create(new CachedFlamdexReader(new MemoryReservationContext(memory), r, "test", "test", cache));
        try (
                final CachedFlamdexReaderReference cfr = new CachedFlamdexReaderReference(resource.getCopy());
                final CachedFlamdexReaderReference cfr1 = new CachedFlamdexReaderReference(resource.getCopy());
                final CachedFlamdexReaderReference cfr2 = new CachedFlamdexReaderReference(resource.getCopy());
        ) {
            IntValueLookup l1 = cfr1.getMetric("m1");
            assertEquals(5L, memory.usedMemory());
            IntValueLookup l2 = cfr2.getMetric("m1");
            assertEquals(5L, memory.usedMemory());
            l1.close();
            assertEquals(5L, memory.usedMemory());
            l2.close();
            assertEquals(0L, memory.usedMemory());
        }
    }

    @Test
    public void testMemory3() throws FlamdexOutOfMemoryException {
        final ImhotepMemoryCache<MetricKey, IntValueLookup> cache = new ImhotepMemoryCache<MetricKey, IntValueLookup>();
        FlamdexReader r = new SillyFlamdexReader();
        MemoryReserver memory = new CachedMemoryReserver(new ImhotepMemoryPool(6L), cache);
        CachedFlamdexReader cfr = new CachedFlamdexReader(new MemoryReservationContext(memory), r, "test", "test", cache);
        cfr.getMetric("m1");
        assertEquals(5L, memory.usedMemory());
        cfr.getMetric("m1");
        assertEquals(5L, memory.usedMemory());
        try {
            cfr.getMetric("m2");
            // error if previous line doesn't throw FOOME
            assertTrue(false);
        } catch (FlamdexOutOfMemoryException e) {
            assertEquals(0L, cache.memoryUsed());
            assertEquals(5L, memory.usedMemory());
        }
    }

    @Test
    public void testLookupWrap() throws FlamdexOutOfMemoryException {
        final ImhotepMemoryCache<MetricKey, IntValueLookup> cache = new ImhotepMemoryCache<MetricKey, IntValueLookup>();
        FlamdexReader r = new SillyFlamdexReader();
        CachedFlamdexReader cfr = new CachedFlamdexReader(new MemoryReservationContext(new CachedMemoryReserver(new ImhotepMemoryPool(Long.MAX_VALUE), cache)), r, "test", "test", cache);
        IntValueLookup l = cfr.getMetric("m1");
        int[] docIds = {0, 2, 4, 9000, Integer.MIN_VALUE};
        long[] values = new long[docIds.length];
        l.lookup(docIds, values, 3);
        assertEquals(Arrays.asList(1L, 9L, 25L, 0L, 0L), Longs.asList(values));
    }

    @Test
    public void testMemoryUsedIsntUsed() throws FlamdexOutOfMemoryException {
        final ImhotepMemoryCache<MetricKey, IntValueLookup> cache = new ImhotepMemoryCache<MetricKey, IntValueLookup>();
        FlamdexReader r = new SillyFlamdexReader();
        MemoryReserver memory = new CachedMemoryReserver(new ImhotepMemoryPool(10L), cache);
        CachedFlamdexReader cfr = new CachedFlamdexReader(new MemoryReservationContext(memory), r, "test", "test", cache);
        IntValueLookup l = cfr.getMetric("‽");
        assertEquals(10L, memory.usedMemory());
        IntValueLookup l2 = cfr.getMetric("‽");
        l.close();
        assertEquals(10L, memory.usedMemory());
        l2.close();
        assertEquals(0L, memory.usedMemory());
    }

    private static class SillyFlamdexReader extends MockFlamdexReader {
        private SillyFlamdexReader() {
            super(Arrays.asList("m1"), Collections.<String>emptyList(), Arrays.asList("m1"), 5);
            super.addIntTerm("m1", 1, Arrays.asList(0));
            super.addIntTerm("m1", 4, Arrays.asList(1));
            super.addIntTerm("m1", 9, Arrays.asList(2));
            super.addIntTerm("m1", 16, Arrays.asList(3));
            super.addIntTerm("m1", 25, Arrays.asList(4));
        }

        @Override
        public IntValueLookup getMetric(String metric) throws FlamdexOutOfMemoryException {
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
                    public void lookup(int[] docIds, long[] values, int n) {
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
                public void lookup(int[] docIds, long[] values, int n) {
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
        public long memoryRequired(String metric) {
            if ("‽".equals(metric)) {
                return 10L;
            }
            return 5L;
        }
    }
}

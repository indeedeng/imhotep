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
 package com.indeed.flamdex.datastruct;

import junit.framework.TestCase;

import java.util.Arrays;
import java.util.Random;

/**
 * @author jsgroth
 */
public class TestFastBitSet extends TestCase {
    public void test1() {
        final FastBitSet bs = new FastBitSet(1000);
        for (int i = 0; i < 1000; i += 3) {
            bs.set(i);
        }
        for (int i = 0; i < 1000; i += 3) {
            assertTrue(bs.get(i));
        }
        bs.clearRange(500, 750);
        for (int i = 0; i < 1000; i += 3) {
            if (i < 500 || i >= 750) {
                assertTrue(bs.get(i));
            }
        }
        for (int i = 500; i < 750; ++i) {
            assertFalse(bs.get(i));
        }
        bs.setRange(500, 750);
        for (int i = 0; i < 1000; i += 3) {
            assertTrue(bs.get(i));
        }
        for (int i = 500; i < 750; ++i) {
            assertTrue(bs.get(i));
        }
    }

    public void test2() {
        final FastBitSet bs = new FastBitSet(128);
        assertEquals(0, bs.cardinality());
        for (int i = 0; i < 64; ++i) {
            bs.setRange(i, i + 64);
            assertEquals(64, bs.cardinality());
            for (int j = 0; j < 128; ++j) {
                if (j < i || j >= i + 64) {
                    assertFalse(bs.get(j));
                } else {
                    assertTrue(bs.get(j));
                }
            }
            bs.clearRange(i + 32, i + 64);
            assertEquals(32, bs.cardinality());
            for (int j = 0; j < 128; ++j) {
                if (j < i || j >= i + 32) {
                    assertFalse(bs.get(j));
                } else {
                    assertTrue(bs.get(j));
                }
            }
            bs.clearRange(i, i + 32);
            assertEquals(0, bs.cardinality());
        }
    }

    public void test3() {
        final FastBitSet bs = new FastBitSet(64);
        assertEquals(0, bs.cardinality());
        bs.setRange(0, 64);
        assertEquals(64, bs.cardinality());
        for (int i = 0; i < 64; ++i) {
            assertTrue(bs.get(i));
        }
        bs.clearRange(0, 64);
        assertEquals(0, bs.cardinality());
        for (int i = 0; i < 64; ++i) {
            assertFalse(bs.get(i));
        }
    }

    public void testIterator() {
        FastBitSet bitSet = new FastBitSet(1024*1024);
        Random r = new Random();
        int[] ints = new int[bitSet.size()/128];
        for (int i = 0; i < ints.length; i++) {
            ints[i] = r.nextInt(bitSet.size());
            bitSet.set(ints[i]);
        }
        Arrays.sort(ints);
        int n = 1;
        for (int i = 1; i < ints.length; i++) {
            if (ints[i] != ints[n-1]) {
                ints[n++] = ints[i];
            }
        }
        FastBitSet.IntIterator iterator = bitSet.iterator();
        for (int i = 0; i < n; i++) {
            assertTrue(iterator.next());
            assertEquals(ints[i], iterator.getValue());
        }
        assertFalse(iterator.next());
    }

    public void testIteratorEnd() {
        final FastBitSet bitSet = new FastBitSet(1);
        bitSet.setAll();
        final FastBitSet.IntIterator iterator = bitSet.iterator();
        assertTrue(iterator.next());
        assertEquals(0, iterator.getValue());
        assertFalse(iterator.next());
    }
}

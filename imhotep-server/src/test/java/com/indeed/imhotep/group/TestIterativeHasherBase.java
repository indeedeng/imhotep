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

package com.indeed.imhotep.group;

import com.google.common.base.Charsets;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

// Base class to test IterativeHasher
// Override createHasher method to test your hasher
public abstract class TestIterativeHasherBase {

    static int SIZE = 1000000;

    private interface Hasher {
        int hash(long value);
    }

    public abstract IterativeHasher createHasher(final String salt);

    public String getSalt() {
        return "someSalt";
    }

    private void testAscDesc(final Hasher hasher,
                             final int size,
                             final long start,
                             final long delta) {
        final int[] asc = new int[size];
        long value = start;
        for (int i = 0; i < size; i++, value += delta) {
            asc[i] = hasher.hash(value);
        }

        final int[] desc = new int[size];
        value -= delta;
        for (int i = size - 1; i >= 0; i--, value -= delta) {
            desc[i] = hasher.hash(value);
        }

        assertArrayEquals(asc, desc);
    }

    private void testAscDesc(final Hasher hasher) {
        testAscDesc(hasher, SIZE, 0, 1 );
        testAscDesc(hasher, SIZE, Long.MIN_VALUE, 1);
        testAscDesc(hasher, SIZE, Long.MAX_VALUE, -1);
        testAscDesc(hasher, SIZE, -SIZE/2, 1 );
        testAscDesc(hasher, SIZE, SIZE/2, -1 );
        testAscDesc(hasher, SIZE, Long.MIN_VALUE, Long.MAX_VALUE / SIZE);
        testAscDesc(hasher, SIZE, Long.MAX_VALUE, -Long.MAX_VALUE / SIZE);
    }

    private boolean testHasher(final Hasher hasher) {
        testAscDesc(hasher);
        return true;
    }

    @Test
    public void testSimpleHasher() {
        final IterativeHasher.SimpleLongHasher simple = createHasher(getSalt()).simpleLongHasher();
        final Hasher hasher = simple::calculateHash;
        assertTrue(testHasher(hasher));
    }

    @Test
    public void testConsistentHasher() {
        final IterativeHasher.ConsistentLongHasher consistent = new IterativeHasher.Murmur3Hasher("someSalt").consistentLongHasher();
        final Hasher hasher = consistent::calculateHash;
        assertTrue(testHasher(hasher));
    }

    @Test
    public void testStringHasher() {
        final IterativeHasher.StringHasher stringHasher = createHasher(getSalt()).stringHasher();
        final Hasher hasher = value -> stringHasher.calculateHash(String.valueOf(value));
        assertTrue(testHasher(hasher));
    }

    @Test
    public void testByteArrayHasher() {
        final IterativeHasher.ByteArrayHasher arrayHasher = createHasher(getSalt()).byteArrayHasher();

        final Hasher hasher = new Hasher() {
            @Override
            public int hash(final long value) {
                final byte[] bytes = String.valueOf(value).getBytes(Charsets.UTF_8);
                return arrayHasher.calculateHash(bytes, bytes.length);
            }
        };
        assertTrue(testHasher(hasher));
    }

    @Test
    public void compareStringAndConsistent() {
        final IterativeHasher.ConsistentLongHasher consistent = createHasher(getSalt()).consistentLongHasher();
        final IterativeHasher.StringHasher stringHasher = createHasher(getSalt()).stringHasher();

        final Hasher hasher = new Hasher() {
            @Override
            public int hash(final long value) {
                final int first = stringHasher.calculateHash(String.valueOf(value));
                final int second = consistent.calculateHash(value);
                assertEquals(first, second);
                return first;
            }
        };
        assertTrue(testHasher(hasher));
    }

    @Test
    public void compareByteArrayAndConsistent() {
        final IterativeHasher.ConsistentLongHasher consistent = createHasher(getSalt()).consistentLongHasher();
        final IterativeHasher.ByteArrayHasher byteArray = createHasher(getSalt()).byteArrayHasher();

        final Hasher hasher = new Hasher() {
            @Override
            public int hash(final long value) {
                final int second = consistent.calculateHash(value);
                final byte[] bytes = String.valueOf(value).getBytes(Charsets.UTF_8);
                final int first = byteArray.calculateHash(bytes, bytes.length);
                assertEquals(first, second);
                return first;
            }
        };
        assertTrue(testHasher(hasher));
    }
}

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

package com.indeed.imhotep.local;

import com.indeed.imhotep.MemoryReserver;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TestGroupLookupFactory {

    private MemoryReserver memory;

    @Before
    public void setUp() {
        // make a dummy MemoryReserver
        memory = new MemoryReserver() {
            @Override
            public long usedMemory() {
                return 0;
            }

            @Override
            public long totalMemory() {
                return 0;
            }

            @Override
            public boolean claimMemory(final long numBytes) {
                return true;
            }

            @Override
            public void releaseMemory(final long numBytes) {
            }

            @Override
            public void close() {
            }
        };
    }

    @After
    public void tearDown() {
        memory.close();
        memory = null;
    }

    // max group for known lookups
    static final int[] MAX_SIZE = { 1, 255, Character.MAX_VALUE, Integer.MAX_VALUE };

    @Test
    public void testBitSetGroupLookup() {
        testLookup(0);
    }

    @Test
    public void testByteGroupLookup() {
        testLookup(1);
    }

    @Test
    public void testCharGroupLookup() {
        testLookup(2);
    }

    @Test
    public void testIntGroupLookup() {
        testLookup(3);
    }

    private void testLookup(final int index) {
        try {
            final GroupLookup lookup = GroupLookupFactory.create(MAX_SIZE[index], 10, memory);

            // lookup is extending to every bigger lookup
            for(int i = index + 1; i < MAX_SIZE.length; i++) {
                final GroupLookup bigger = GroupLookupFactory.resize(lookup, MAX_SIZE[i], memory);
                assertTrue(lookup.maxGroup() < bigger.maxGroup());
            }

            // lookup stays the same if maxGroup is not changing
            final GroupLookup same = GroupLookupFactory.resize(lookup, lookup.maxGroup(), memory);
            assertSame(same, lookup);

            // lookup is shrinking to every lower lookup
            for(int i = 0; i < index; i++) {
                final GroupLookup smaller = GroupLookupFactory.resize(lookup, MAX_SIZE[i], memory);
                assertTrue(smaller.maxGroup() < lookup.maxGroup());
            }
        } catch (final ImhotepOutOfMemoryException e) {
            fail();
        }
    }
}

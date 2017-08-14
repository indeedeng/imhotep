package com.indeed.imhotep.local;

import com.indeed.imhotep.MemoryReserver;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
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

    @Test
    public void testBitSetGroupLookup() {
        testLookup(1);
    }

    @Test
    public void testByteGroupLookup() {
        testLookup(255);
    }

    @Test
    public void testCharGroupLookup() {
        testLookup(65535);
    }

    @Test
    public void testIntGroupLookup() {
        testLookup(65536);
    }

    private void testLookup(final int groupCount) {
        try {
            final GroupLookup lookup = GroupLookupFactory.create(groupCount, 10, null, memory);

            // lookup is extending if we overflow current max size
            if (lookup.maxGroup() < Integer.MAX_VALUE) {
                final GroupLookup big = GroupLookupFactory.resize(lookup, lookup.maxGroup() + 1, memory);
                assertTrue(lookup.maxGroup() < big.maxGroup());
            }

            // lookup stays the same if maxGroup is not changing
            final GroupLookup same = GroupLookupFactory.resize(lookup, lookup.maxGroup(), memory);
            assertSame(same, lookup);

            // lookup is shrinking to BitSetGroupLookup
            final GroupLookup small = GroupLookupFactory.resize(lookup, -1, memory);
            assertEquals(1, small.maxGroup());
        } catch (final ImhotepOutOfMemoryException e) {
            fail();
        }
    }
}

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

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * @author jsgroth
 */
public class TestMMapFastBitSet {
    private Path tmp;

    @Before
    public void setUp() throws IOException {
        tmp = Files.createTempFile("asdf", "");
    }

    @After
    public void tearDown() throws IOException {
        Files.delete(tmp);
    }

    @Test
    public void test1() throws IOException {
        final MMapFastBitSet bs = new MMapFastBitSet(tmp, 1000, FileChannel.MapMode.READ_WRITE);
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
        bs.close();
    }

    @Test
    public void test2() throws IOException {
        final MMapFastBitSet bs = new MMapFastBitSet(tmp, 128, FileChannel.MapMode.READ_WRITE);
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
        bs.close();
    }

    @Test
    public void test3() throws IOException {
        final MMapFastBitSet bs = new MMapFastBitSet(tmp, 64, FileChannel.MapMode.READ_WRITE);
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
        bs.close();
    }
}

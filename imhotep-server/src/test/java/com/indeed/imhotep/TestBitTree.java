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
 package com.indeed.imhotep;

import org.apache.log4j.Logger;
import org.junit.Test;

import java.util.BitSet;
import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * @author jplaisance
 */
public final class TestBitTree {
    private static final Logger log = Logger.getLogger(TestBitTree.class);

    @Test
    public void testBitTree() {
        final Random r = new Random(0);
        for (int i = 0; i < 30; i++) {
            for (int density = 1; density <= 16; density++) {
                final int size = r.nextInt((1<<i))+1;
                final BitSet bs = new BitSet(size);
                final BitTree tree = new BitTree(size);
                for (int j = 0; j < size/(1<<density); j++) {
                    final int rand = r.nextInt(size);
                    bs.set(rand);
                    tree.set(rand);
                }
                for (int j = bs.nextSetBit(0); j >= 0; j = bs.nextSetBit(j+1)) {
                    assertTrue(tree.next());
                    assertEquals(j, tree.getValue());
                }
                assertFalse(tree.next());
            }
        }
    }
}

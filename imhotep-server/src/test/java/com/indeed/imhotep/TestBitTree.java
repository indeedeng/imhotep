package com.indeed.imhotep;

import junit.framework.TestCase;
import org.apache.log4j.Logger;

import java.util.BitSet;
import java.util.Random;

/**
 * @author jplaisance
 */
public final class TestBitTree extends TestCase {
    private static final Logger log = Logger.getLogger(TestBitTree.class);

    public void testBitTree() {
        final Random r = new Random(0);
        for (int i = 0; i < 30; i++) {
            for (int density = 1; density <= 16; density++) {
                final int size = r.nextInt((1<<i))+1;
                BitSet bs = new BitSet(size);
                BitTree tree = new BitTree(size);
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

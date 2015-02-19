package com.indeed;

import java.util.BitSet;
import java.util.Random;
import java.lang.String;
import java.lang.System;
import java.io.IOException;

/**
 * @author jplaisance
 */
public final class TestBitTree {

    public static void main(String[] args) throws IOException {
        boolean first = true;
        
        final Random r = new Random(0);
        for (int i = 0; i < 30; i++) {
            for (int density = 1; density <= 16; density++) {
                final int size = r.nextInt((1<<i))+1;
                BitSet bs = new BitSet(size);
                BitTreeNative tree = new BitTreeNative(size);
                if (first) {
                    System.in.read();
                    first = false;
                }
                for (int j = 0; j < size/(1<<density); j++) {
                    final int rand = r.nextInt(size);
                    bs.set(rand);
                    tree.set(rand);
                }
                for (int j = bs.nextSetBit(0); j >= 0; j = bs.nextSetBit(j+1)) {
                    assert tree.next();
                    assert (j == tree.getValue());
                }
                assert !tree.next();
            }
        }
    }
}

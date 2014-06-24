package com.indeed.imhotep;

import org.apache.log4j.Logger;

/**
 * @author jplaisance
 */
public final class BitTree {
    private static final Logger log = Logger.getLogger(BitTree.class);

    final long[][] bitsets;
    boolean cleared = true;

    final int[] indexStack;
    int depth;
    int current;

    public BitTree(int size) {
        bitsets = new long[log2(size-1)/6+1][];
        for (int i = 0; i < bitsets.length; i++) {
            size = (size+63)/64;
            bitsets[i] = new long[size];
        }
        indexStack = new int[bitsets.length];
        depth = indexStack.length-1;
    }

    private static int log2(final int size) {
        return 31-Integer.numberOfLeadingZeros(size);
    }

    public void set(int index) {
        if (!cleared) throw new IllegalStateException();
        for (int i = 0; i < bitsets.length; i++) {
            final int nextIndex = index>>>6;
            bitsets[i][nextIndex] |= 1L<<(index&0x3F);
            index = nextIndex;
        }
    }

    public void set(int[] indexes, int n) {
        if (!cleared) throw new IllegalStateException();
        for (int j = 0; j < n; j++) {
            int index = indexes[j];
            for (int i = 0; i < bitsets.length; i++) {
                final int nextIndex = index>>>6;
                bitsets[i][nextIndex] |= 1L<<(index&0x3F);
                index = nextIndex;
            }
        }
    }

    public boolean get(int index) {
        return (bitsets[0][index>>6]&(1L<<(index&0x3F))) != 0;
    }

    public void clear() {
        cleared = true;
        while (true) {
            while (bitsets[depth][indexStack[depth]] == 0) {
                if (depth == bitsets.length-1) {
                    return;
                }
                depth++;
            }
            while (depth != 0) {
                final long lsb = bitsets[depth][indexStack[depth]] & -bitsets[depth][indexStack[depth]];
                bitsets[depth][indexStack[depth]] ^= lsb;
                final int index = indexStack[depth];
                depth--;
                indexStack[depth] = (index<<6)+Long.bitCount(lsb-1);
            }
            bitsets[0][indexStack[0]] = 0;
            depth = 1;
        }
    }

    public boolean next() {
        cleared = false;
        long bits = bitsets[depth][indexStack[depth]];
        while (bits == 0) {
            if (depth == bitsets.length-1) {
                return false;
            }
            depth++;
            bits = bitsets[depth][indexStack[depth]];
        }
        while (depth != 0) {
            final long lsb = bits & -bits;
            bitsets[depth][indexStack[depth]] ^= lsb;
            final int index = indexStack[depth];
            depth--;
            indexStack[depth] = (index<<6)+Long.bitCount(lsb-1);
            bits = bitsets[depth][indexStack[depth]];
        }
        final long lsb = bits & -bits;
        final int index = indexStack[0];
        bitsets[0][index] ^= lsb;
        current = (index<<6)+Long.bitCount(lsb-1);
        return true;
    }

    public int getValue() {
        return current;
    }
}

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

import com.indeed.imhotep.api.FTGSIterator;
import com.indeed.util.core.io.Closeables2;
import org.apache.log4j.Logger;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.util.Arrays;
import java.util.Collection;

/**
 * @author jsgroth
 */
public final class FastIntFTGSMerger implements FTGSIterator {
    private static final Logger log = Logger.getLogger(FastIntFTGSMerger.class);

    private final FTGSIterator[] iterators;
    private int numFieldIterators = 0;

    private String fieldName;
    private boolean fieldIsIntType;
    private long termIntVal;

    private boolean done;

    private final int numGroups;
    private final Closeable doneCallback;

    private final GSVector accumulatedVec;

    public FastIntFTGSMerger(
            final Collection<? extends FTGSIterator> iterators,
            final int numStats,
            final int numGroups,
            @Nullable final Closeable doneCallback) {
        this.numGroups = numGroups;
        this.doneCallback = doneCallback;
        this.iterators = iterators.toArray(new FTGSIterator[iterators.size()]);
        done = false;
        accumulatedVec = new GSVector(numStats, numGroups);
    }

    @Override
    public boolean nextField() {
        if (done) {
            return false;
        }

        final FTGSIterator first = iterators[0];
        if (!first.nextField()) {
            for (int i = 1; i < iterators.length; ++i) {
                if (iterators[i].nextField()) {
                    throw new IllegalArgumentException("sub iterator fields do not match");
                }
            }
            close();
            return false;
        }
        fieldName = first.fieldName();
        fieldIsIntType = first.fieldIsIntType();
        numFieldIterators = iterators.length;

        for (int i = 1; i < iterators.length; ++i) {
            final FTGSIterator itr = iterators[i];
            if (!itr.nextField() || !itr.fieldName().equals(fieldName) || itr.fieldIsIntType() != fieldIsIntType) {
                throw new IllegalArgumentException("sub iterator fields do not match");
            }
        }
        for (int i = iterators.length-1; i >= 0; i--) {
            while (true) {
                if (!iterators[i].nextTerm()) {
                    numFieldIterators--;
                    swap(iterators, i, numFieldIterators);
                    break;
                }
                if (!iterators[i].nextGroup()) {
                    continue;
                }
                break;
            }
        }

        accumulatedVec.resetNewField();

        return true;
    }

    private static void swap(final Object[] array, final int indexA, final int indexB) {
        final Object a = array[indexA];
        array[indexA] = array[indexB];
        array[indexB] = a;
    }

    @Override
    public String fieldName() {
        return fieldName;
    }

    @Override
    public boolean fieldIsIntType() {
        return fieldIsIntType;
    }

    private void refill() {
        long minBaseTermGroup = Long.MAX_VALUE;
        for (int i = 0; i < numFieldIterators; i++) {
            final long baseTermGroup = (iterators[i].termIntVal()*numGroups+iterators[i].group())&~0xFFF;
            if (baseTermGroup < minBaseTermGroup) {
                minBaseTermGroup = baseTermGroup;
            }
        }
        accumulatedVec.reset();
        for (int i = numFieldIterators-1; i >= 0; i--) {
            final long baseTermGroup = (iterators[i].termIntVal()*numGroups+iterators[i].group())&~0xFFF;
            if (baseTermGroup == minBaseTermGroup) {
                if (!accumulatedVec.mergeFromFtgs(iterators[i])) {
                    numFieldIterators--;
                    swap(iterators, i, numFieldIterators);
                }
            }
        }
    }

    @Override
    public boolean nextTerm() {
        while (true) {
            if (accumulatedVec.nextTerm()) {
                return true;
            }
            if (numFieldIterators == 0) {
                return false;
            }
            refill();
        }
    }

    @Override
    public long termDocFreq() {
        return 1;
    }

    @Override
    public long termIntVal() {
        return accumulatedVec.term;
    }

    public String termStringVal() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean nextGroup() {
        if (accumulatedVec.nextGroup()) {
            return true;
        }
        if (accumulatedVec.bitset1 == 0 && accumulatedVec.bitset2[accumulatedVec.iteratorIndex] == 0) {
            if (numFieldIterators == 0) {
                return false;
            }
            refill();
            return accumulatedVec.nextGroup();
        }
        return false;
    }

    @Override
    public int group() {
        return accumulatedVec.group();
    }

    @Override
    public void groupStats(final long[] stats) {
        accumulatedVec.groupStats(stats);
    }

    @Override
    public synchronized void close() {
        if (!done) {
            done = true;
            Closeables2.closeAll(log, Closeables2.forArray(log, iterators), doneCallback);
        }
    }

    private static void swap(final int[] a, final int b, final int e) {
        final int t = a[b];
        a[b] = a[e];
        a[e] = t;
    }

    static final class GSVector {

        long bitset1;
        final long[] bitset2 = new long[64];
        final long[] metrics;

        private final int numStats;
        private final int numGroups;

        private long base = -1;

        private int iteratorIndex = -1;
        private long term = 0;
        private int group = -1;

        private final long[] statBuf;

        public GSVector(final int numStats, final int numGroups) {
            this.numStats = numStats;
            this.numGroups = numGroups;
            metrics = new long[numStats*4096];
            statBuf = new long[numStats];
        }

        public void resetNewField() {
            reset();
            term = Long.MIN_VALUE;
        }

        public void reset() {
            if (group >= 0) {
                final int start = (int)(term*numGroups+group-base) * numStats;
                Arrays.fill(metrics, start, start+numStats, 0);
            }
            while (bitset1 != 0) {
                final long lsb1 = bitset1 & -bitset1;
                bitset1 ^= lsb1;
                final int index1 = Long.bitCount(lsb1-1);
                while (bitset2[index1] != 0) {
                    final long lsb2 = bitset2[index1] & -bitset2[index1];
                    bitset2[index1] ^= lsb2;
                    final int index2 = (index1<<6)+Long.bitCount(lsb2-1);
                    Arrays.fill(metrics, index2 * numStats, index2 * numStats + numStats, 0);
                }
            }
            base = -1;
            iteratorIndex = -1;
            group = -1;
        }

        public boolean mergeFromFtgs(final FTGSIterator ftgs) {
            final long termGroup = ftgs.termIntVal()*numGroups+ftgs.group();
            final long newBase = termGroup & ~0xFFF;
            if (base != -1 && base != newBase) {
                throw new IllegalStateException();
            }
            base = newBase;
            int termGroupOffset = (int)(termGroup-base);
            do {
                ftgs.groupStats(statBuf);
                for (int i = 0; i < numStats; i++) {
                    metrics[termGroupOffset*numStats+i] += statBuf[i];
                }
                final int bitset2index = termGroupOffset>>>6;
                bitset1 |= 1L<<bitset2index;
                bitset2[bitset2index] |= 1L<<(termGroupOffset&0x3F);
                while (true) {
                    if (!ftgs.nextGroup()) {
                        if (!ftgs.nextTerm()) {
                            return false;
                        }
                        continue;
                    }
                    break;
                }
                termGroupOffset = (int) (ftgs.termIntVal()*numGroups+ftgs.group() - base);
            } while (termGroupOffset < 4096);
            return true;
        }

        //clears bitsets and metrics as it iterates
        public boolean nextGroup() {
            if (group >= 0) {
                final int start = (int)((term*numGroups+group-base) * numStats);
                Arrays.fill(metrics, start, start+numStats, 0);
            }
            if (iteratorIndex < 0 || bitset2[iteratorIndex] == 0) {
                if (bitset1 == 0) {
                    return false;
                }
                final long lsb1 = bitset1 & -bitset1;
                bitset1 ^= lsb1;
                iteratorIndex = Long.bitCount(lsb1-1);
            }
            final long lsb2 = bitset2[iteratorIndex] & -bitset2[iteratorIndex];
            final int bitset2index = Long.bitCount(lsb2 - 1);
            final int groupOffset = (iteratorIndex << 6) | bitset2index;
            final long termGroup = base+groupOffset;
            if (termGroup >= term*numGroups+numGroups) {
                return false;
            }
            bitset2[iteratorIndex] ^= lsb2;
            group = (int) (termGroup-term*numGroups);
            return true;
        }

        public boolean nextTerm() {
            while (nextGroup()) {
                //finish previous term
            }
            if (iteratorIndex < 0 || bitset2[iteratorIndex] == 0) {
                if (bitset1 == 0) {
                    return false;
                }
                final long lsb1 = bitset1 & -bitset1;
                bitset1 ^= lsb1;
                iteratorIndex = Long.bitCount(lsb1-1);
            }
            final long lsb2 = bitset2[iteratorIndex] & -bitset2[iteratorIndex];
            final int bitset2index = Long.bitCount(lsb2 - 1);
            final int groupOffset = (iteratorIndex << 6) | bitset2index;
            final long termGroup = base+groupOffset;
            term = lfloordiv(termGroup, numGroups);
            group = -1;
            return true;
        }

        public static long lfloordiv(final long n, final long d) {
            if (n >= 0) {
                return n / d;
            } else {
                return ~(~n / d);
            }
        }

        public long term() {
            return term;
        }

        public int group() {
            return group;
        }

        public void groupStats(final long[] buf) {
            System.arraycopy(metrics, (int) ((term*numGroups+group-base)*numStats), buf, 0, numStats);
        }

        public String toString() {
            final StringBuilder ret = new StringBuilder(String.format("%64s", Long.toBinaryString(bitset1)).replace(' ', '0') + "\n");
            long tmpBitset1 = bitset1;
            while (tmpBitset1 != 0) {
                final long lsb = tmpBitset1 & -tmpBitset1;
                final int index = Long.bitCount(lsb-1);
                tmpBitset1 ^= lsb;
                ret.append(String.format("%64s", Long.toBinaryString(bitset2[index])).replace(' ', '0')).append("\n");
            }
            return ret.toString();
        }
    }
}

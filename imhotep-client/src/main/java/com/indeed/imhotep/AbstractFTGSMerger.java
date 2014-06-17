package com.indeed.imhotep;

import com.indeed.util.core.io.Closeables2;
import com.indeed.imhotep.api.FTGSIterator;
import org.apache.log4j.Logger;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.util.Arrays;
import java.util.Collection;

/**
 * @author jsgroth
 */
public abstract class AbstractFTGSMerger implements FTGSIterator {
    private static final Logger log = Logger.getLogger(AbstractFTGSMerger.class);

    protected final FTGSIterator[] iterators;
    private final int numIterators;
    protected final int[] fieldIterators;
    protected int numFieldIterators;
    protected final int[] termIterators;
    protected final int[] termIteratorIndexes;
    protected int numTermIterators;
    protected int termIteratorsRemaining;

    private String fieldName;
    protected boolean fieldIsIntType;
    protected long termIntVal;

    private boolean done;

    private final Closeable doneCallback;

    protected final GSVector accumulatedVec;

    protected AbstractFTGSMerger(Collection<? extends FTGSIterator> iterators, int numStats, @Nullable Closeable doneCallback) {
        this.doneCallback = doneCallback;
        numIterators = iterators.size();
        this.iterators = iterators.toArray(new FTGSIterator[numIterators]);
        fieldIterators = new int[numIterators];
        numFieldIterators = 0;
        termIterators = new int[numIterators];
        termIteratorIndexes = new int[numIterators];
        numTermIterators = 0;
        done = false;
        accumulatedVec = new GSVector(numStats);
    }

    @Override
    public final boolean nextField() {
        if (done) return false;

        numFieldIterators = 0;

        final FTGSIterator first = iterators[0];
        if (!first.nextField()) {
            for (int i = 1; i < numIterators; ++i) {
                if (iterators[i].nextField()) {
                    throw new IllegalArgumentException("sub iterator fields do not match");
                }
            }
            close();
            return false;
        }
        fieldName = first.fieldName();
        fieldIsIntType = first.fieldIsIntType();
        numFieldIterators = 0;
        if (first.nextTerm()) {
            fieldIterators[numFieldIterators++] = 0;
        }

        for (int i = 1; i < numIterators; ++i) {
            final FTGSIterator itr = iterators[i];
            if (!itr.nextField() || !itr.fieldName().equals(fieldName) || itr.fieldIsIntType() != fieldIsIntType) {
                throw new IllegalArgumentException("sub iterator fields do not match");
            }
            if (itr.nextTerm()) {
                fieldIterators[numFieldIterators++] = i;
            }
        }

        return true;
    }

    @Override
    public final String fieldName() {
        return fieldName;
    }

    @Override
    public final boolean fieldIsIntType() {
        return fieldIsIntType;
    }

    @Override
    public final long termDocFreq() {
        long ret = 0L;
        for (int i = 0; i < termIteratorsRemaining; ++i) {
            ret += iterators[termIterators[i]].termDocFreq();
        }
        return ret;
    }

    @Override
    public final long termIntVal() {
        return termIntVal;
    }

    @Override
    public final boolean nextGroup() {
        while (true) {
            if (accumulatedVec.nextGroup()) {
                return true;
            }
            if (termIteratorsRemaining == 0) return false;

            int baseGroup = Integer.MAX_VALUE;
            for (int i = 0; i < termIteratorsRemaining; ++i) {
                final FTGSIterator itr = iterators[termIterators[i]];
                final int group = itr.group()&0xFFFFF000;
                if (group < baseGroup) {
                    baseGroup = group;
                }
            }

            accumulatedVec.reset();
            for (int i = 0; i < termIteratorsRemaining; ++i) {
                final FTGSIterator itr = iterators[termIterators[i]];
                if ((itr.group()&0xFFFFF000) == baseGroup) {
                    if (!accumulatedVec.mergeFromFtgs(itr)) {
                        swap(termIterators, i, --termIteratorsRemaining);
                        swap(termIteratorIndexes, i, termIteratorsRemaining);
                        --i;
                    }
                }
            }
        }
    }

    @Override
    public final int group() {
        return accumulatedVec.group();
    }

    @Override
    public final void groupStats(long[] stats) {
        accumulatedVec.groupStats(stats);
    }

    @Override
    public synchronized void close() {
        if (!done) {
            done = true;
            Closeables2.closeAll(log, Closeables2.forArray(log, iterators), doneCallback);
        }
    }

    protected static void swap(final int[] a, final int b, final int e) {
        final int t = a[b];
        a[b] = a[e];
        a[e] = t;
    }

    static final class GSVector {

        long bitset1;
        final long[] bitset2 = new long[64];
        final long[] metrics;

        private final int numStats;

        private int baseGroup = -1;

        private int iteratorIndex;
        private int group;

        private final long[] statBuf;

        public GSVector(final int numStats) {
            this.numStats = numStats;
            metrics = new long[numStats*4096];
            statBuf = new long[numStats];
        }

        public void reset() {
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
            baseGroup = -1;
            iteratorIndex = -1;
            group = -1;
        }

        public boolean mergeFromFtgs(FTGSIterator ftgs) {
            int group = ftgs.group();
            final int newBaseGroup = group & 0xFFFFF000;
            if (baseGroup != -1 && baseGroup != newBaseGroup) {
                throw new IllegalStateException();
            }
            baseGroup = newBaseGroup;
            group -= baseGroup;
            do {
                ftgs.groupStats(statBuf);
                for (int i = 0; i < numStats; i++) {
                    metrics[group*numStats+i] += statBuf[i];
                }
                final int bitset2index = group>>>6;
                bitset1 |= 1L<<bitset2index;
                bitset2[bitset2index] |= 1L<<(group&0x3F);
                if (!ftgs.nextGroup()) return false;
                group = ftgs.group()-baseGroup;
            } while (group < 4096);
            return true;
        }

        //clears bitsets and metrics as it iterates
        public boolean nextGroup() {
            if (group >= 0) {
                final int start = (group - baseGroup) * numStats;
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
            bitset2[iteratorIndex] ^= lsb2;
            final int bitset2index = Long.bitCount(lsb2 - 1);
            final int groupOffset = (iteratorIndex << 6) | bitset2index;
            group = baseGroup+groupOffset;
            return true;
        }

        public int group() {
            return group;
        }

        public void groupStats(long buf[]) {
            System.arraycopy(metrics, (group-baseGroup)*numStats, buf, 0, numStats);
        }
    }
}

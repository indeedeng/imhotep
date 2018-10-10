package com.indeed.imhotep;

import com.indeed.imhotep.api.FTGSIterator;
import com.indeed.imhotep.metrics.aggregate.MultiFTGSIterator;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.util.Arrays;
import java.util.List;

/**
 * @author jwolfe
 */
public class MultiSessionMerger extends AbstractBatchedFTGMerger implements MultiFTGSIterator {
    private final GSVector accumulatedVec;

    public MultiSessionMerger(List<? extends FTGSIterator> sessionIterators, @Nullable Closeable doneCallback) {
        super(sessionIterators, doneCallback, true);
        accumulatedVec = new GSVector(sessionIterators);
    }

    @Override
    public String fieldName() {
        return "magic";
    }

    @Override
    boolean nextBatchedGroup() {
        return accumulatedVec.nextGroup();
    }

    @Override
    void calculateNextGroupBatch() {
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
                    termIteratorsRemaining--;
                    swap(termIterators, i, termIteratorsRemaining);
                    swap(termIteratorIndexes, i, termIteratorsRemaining);
                    --i;
                }
            }
        }
    }

    @Override
    void resetBatchedGroups() {
        accumulatedVec.reset();
    }

    @Override
    public int group() {
        return accumulatedVec.group();
    }

    @Override
    public void groupStats(int sessionIndex, long[] stats) {
        accumulatedVec.groupStats(sessionIndex, stats);
    }

    @Override
    public long groupStat(int sessionIndex, int statIndex) {
        return accumulatedVec.groupStat(sessionIndex, statIndex);
    }

    static final class GSVector {
        long bitset1;
        final long[] bitset2 = new long[64];
        final long[] metrics;

        private final int totalNumStats;
        private final List<? extends FTGSIterator> iterators;

        private int baseGroup = -1;

        private int iteratorIndex;
        private int group;

        private final long[] statBuf;
        private final int[] numStats;
        private final int[] statOffsets;

        public GSVector(final List<? extends FTGSIterator> iterators) {
            numStats = new int[iterators.size()];
            statOffsets = new int[iterators.size()];

            int totalNumStats = 0;
            for (int i = 0; i < iterators.size(); i++) {
                final int iterNumStats = iterators.get(i).getNumStats();
                this.statOffsets[i] = totalNumStats;
                this.numStats[i] = iterNumStats;
                totalNumStats += iterNumStats;
            }

            this.totalNumStats = totalNumStats;

            this.iterators = iterators;
            metrics = new long[totalNumStats * 4096];
            statBuf = new long[totalNumStats];
        }

        public void reset() {
            if (group >= 0) {
                final int start = (group - baseGroup) * totalNumStats;
                Arrays.fill(metrics, start, start + totalNumStats, 0);
            }
            while (bitset1 != 0) {
                final long lsb1 = bitset1 & -bitset1;
                bitset1 ^= lsb1;
                final int index1 = Long.bitCount(lsb1 - 1);
                while (bitset2[index1] != 0) {
                    final long lsb2 = bitset2[index1] & -bitset2[index1];
                    bitset2[index1] ^= lsb2;
                    final int index2 = (index1 << 6) + Long.bitCount(lsb2 - 1);
                    Arrays.fill(metrics, index2 * totalNumStats, index2 * totalNumStats + totalNumStats, 0);
                }
            }
            baseGroup = -1;
            iteratorIndex = -1;
            group = -1;
        }

        public boolean mergeFromFtgs(final FTGSIterator ftgs) {
            final int sessionIndex = iterators.indexOf(ftgs);
            final int sessionOffset = statOffsets[sessionIndex];
            final int sessionNumStats = numStats[sessionIndex];

            int group = ftgs.group();
            final int newBaseGroup = group & 0xFFFFF000;
            if (baseGroup != -1 && baseGroup != newBaseGroup) {
                throw new IllegalStateException();
            }
            baseGroup = newBaseGroup;
            group -= baseGroup;
            do {
                ftgs.groupStats(statBuf);
                for (int i = 0; i < sessionNumStats; i++) {
                    metrics[group * totalNumStats + sessionOffset + i] += statBuf[i];
                }
                final int bitset2index = group >>> 6;
                bitset1 |= 1L << bitset2index;
                bitset2[bitset2index] |= 1L << (group & 0x3F);
                if (!ftgs.nextGroup()) {
                    return false;
                }
                group = ftgs.group() - baseGroup;
            } while (group < 4096);
            return true;
        }

        //clears bitsets and metrics as it iterates
        public boolean nextGroup() {
            if (group >= 0) {
                final int start = (group - baseGroup) * totalNumStats;
                Arrays.fill(metrics, start, start + totalNumStats, 0);
            }
            if (iteratorIndex < 0 || bitset2[iteratorIndex] == 0) {
                if (bitset1 == 0) {
                    return false;
                }
                final long lsb1 = bitset1 & -bitset1;
                bitset1 ^= lsb1;
                iteratorIndex = Long.bitCount(lsb1 - 1);
            }
            final long lsb2 = bitset2[iteratorIndex] & -bitset2[iteratorIndex];
            bitset2[iteratorIndex] ^= lsb2;
            final int bitset2index = Long.bitCount(lsb2 - 1);
            final int groupOffset = (iteratorIndex << 6) | bitset2index;
            group = baseGroup + groupOffset;
            return true;
        }

        public int group() {
            return group;
        }

        public void groupStats(final int sessionIndex, final long[] buf) {
            final int sessionOffset = statOffsets[sessionIndex];
            final int sessionNumStats = numStats[sessionIndex];
            System.arraycopy(metrics, (group - baseGroup) * totalNumStats + sessionOffset, buf, 0, sessionNumStats);
        }

        long groupStat(int sessionIndex, int statIndex) {
            final int sessionOffset = statOffsets[sessionIndex];
            return metrics[(group - baseGroup) * totalNumStats + sessionOffset + statIndex];
        }
    }

}

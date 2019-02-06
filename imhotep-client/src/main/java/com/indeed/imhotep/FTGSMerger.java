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
 package com.indeed.imhotep;

import com.indeed.imhotep.api.FTGSIterator;
import org.apache.log4j.Logger;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.util.Arrays;
import java.util.Collection;

/**
 * @author jsgroth
 */
public class FTGSMerger extends AbstractBatchedFTGMerger implements FTGSIterator {
    private static final Logger log = Logger.getLogger(FTGSMerger.class);

    private final int numStats;
    private final GSVector accumulatedVec;

    public FTGSMerger(
            final Collection<? extends FTGSIterator> iterators,
            @Nullable final Closeable doneCallback) {
        super(iterators, doneCallback, false);
        numStats = FTGSIteratorUtil.getNumStats(this.iterators);
        accumulatedVec = new GSVector(numStats);
    }

    @Override
    public int getNumStats() {
        return numStats;
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
    public final int group() {
        return accumulatedVec.group();
    }

    @Override
    public final void groupStats(final long[] stats, final int offset) {
        accumulatedVec.groupStats(stats, offset);
    }


    final class GSVector {
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
            metrics = new long[numStats * 4096];
            statBuf = new long[numStats];
        }

        public void reset() {
            if (group >= 0) {
                final int start = (group - baseGroup) * numStats;
                Arrays.fill(metrics, start, start + numStats, 0);
            }
            while (bitset1 != 0) {
                final long lsb1 = bitset1 & -bitset1;
                bitset1 ^= lsb1;
                final int index1 = Long.bitCount(lsb1 - 1);
                while (bitset2[index1] != 0) {
                    final long lsb2 = bitset2[index1] & -bitset2[index1];
                    bitset2[index1] ^= lsb2;
                    final int index2 = (index1 << 6) + Long.bitCount(lsb2 - 1);
                    Arrays.fill(metrics, index2 * numStats, index2 * numStats + numStats, 0);
                }
            }
            baseGroup = -1;
            iteratorIndex = -1;
            group = -1;
        }

        public boolean mergeFromFtgs(final FTGSIterator ftgs) {
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
                    metrics[group * numStats + i] += statBuf[i];
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
                final int start = (group - baseGroup) * numStats;
                Arrays.fill(metrics, start, start + numStats, 0);
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

        public void groupStats(final long[] buf, final int offset) {
            System.arraycopy(metrics, (group - baseGroup) * numStats, buf, offset, numStats);
        }
    }
}

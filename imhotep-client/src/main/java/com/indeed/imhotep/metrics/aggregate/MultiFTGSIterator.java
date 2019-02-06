package com.indeed.imhotep.metrics.aggregate;

import com.indeed.imhotep.api.FTGIterator;

/**
 * @author jwolfe
 */
public interface MultiFTGSIterator extends FTGIterator {
    long groupStat(int sessionIndex, int statIndex);

    default void groupStats(final int sessionIndex, final long[] stats) {
        groupStats(sessionIndex, stats, 0);
    }

    void groupStats(int sessionIndex, long[] stats, int offset);

    int[] numStats();
}

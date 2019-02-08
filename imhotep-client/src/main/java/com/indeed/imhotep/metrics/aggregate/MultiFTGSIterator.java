package com.indeed.imhotep.metrics.aggregate;

import com.indeed.imhotep.api.FTGIterator;
import com.indeed.imhotep.api.FTGSIterator;

/**
 * @author jwolfe
 */
public interface MultiFTGSIterator extends FTGIterator {
    /**
     * Return the stat for a given session at the given index.
     */
    long groupStat(int sessionIndex, int statIndex);

    /**
     * Copy the stats for a given session into the stats buffer, as per {@link FTGSIterator#groupStats(long[])}
     */
    default void groupStats(final int sessionIndex, final long[] stats) {
        groupStats(sessionIndex, stats, 0);
    }

    /**
     * Copy the stats for a given session into the stats buffer with offset, as per {@link FTGSIterator#groupStats(long[], int)}
     */
    void groupStats(int sessionIndex, long[] stats, int offset);

    /**
     * @return an array where each element indicates the number of stats for the session at that index, as per {@link FTGSIterator#getNumStats()}
     */
    int[] numStats();
}

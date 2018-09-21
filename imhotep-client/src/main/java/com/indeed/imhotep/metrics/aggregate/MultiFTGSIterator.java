package com.indeed.imhotep.metrics.aggregate;

import com.indeed.imhotep.api.FTGIterator;

/**
 * @author jwolfe
 */
public interface MultiFTGSIterator extends FTGIterator {
    long groupStat(int sessionIndex, int statIndex);
    void groupStats(int sessionIndex, long[] stats);
}

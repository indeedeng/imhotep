package com.indeed.imhotep.api;

import com.indeed.imhotep.FTGSIteratorUtil;
import com.indeed.imhotep.TermLimitedFTGAIterator;
import com.indeed.imhotep.TermLimitedFTGSIterator;
import com.indeed.imhotep.protobuf.SortOrder;
import com.indeed.imhotep.scheduling.SilentCloseable;
import com.indeed.imhotep.scheduling.TaskScheduler;

import java.io.Closeable;
import java.io.IOException;

/**
 *  Class for getFTGSIterator method params
 */
public class FTGSModifiers {
    public final long termLimit;
    public final int sortStat;
    public final boolean sorted;
    public final SortOrder sortOrder;

    /**
     * @param termLimit - see {@link ImhotepSession#getFTGSIterator(FTGSParams)} for details
     * @param sortStat - see {@link ImhotepSession#getFTGSIterator(FTGSParams)} for details
     * @param sorted - see {@link ImhotepSession#getFTGSIterator(FTGSParams)} for details
     */
    public FTGSModifiers(
            final long termLimit,
            final int sortStat,
            final boolean sorted,
            final SortOrder sortOrder
    ){
        if (termLimit < 0) {
            throw new IllegalArgumentException("termLimit must be non-negative");
        }

        this.termLimit = termLimit;
        this.sortStat = sortStat;
        this.sorted = sorted;
        this.sortOrder = sortOrder;
    }

    public boolean isTopTerms() {
        return (sortStat >= 0) && (termLimit > 0);
    }

    public boolean isTermLimit() {
        return (sortStat < 0) && (termLimit > 0);
    }

    public FTGSModifiers copy() {
        return new FTGSModifiers(termLimit, sortStat, sorted, sortOrder);
    }

    public FTGSModifiers sortedCopy() {
        return new FTGSModifiers(termLimit, sortStat, true, sortOrder);
    }

    public FTGSModifiers unsortedCopy() {
        return new FTGSModifiers(termLimit, sortStat, false, sortOrder);
    }

    public FTGSModifiers unlimitedCopy() {
        return new FTGSModifiers(0, -1, sorted, sortOrder);
    }

    public FTGSIterator wrap(FTGSIterator iterator) throws IOException {
        if (termLimit > 0) {
            if (sortStat >= 0) {
                try(final Closeable ignored = TaskScheduler.CPUScheduler.lockSlot()) {
                    return FTGSIteratorUtil.getTopTermsFTGSIterator(iterator, termLimit, sortStat, sortOrder);
                }
            } else {
                return new TermLimitedFTGSIterator(iterator, termLimit);
            }
        } else {
            return iterator;
        }
    }

    public FTGAIterator wrap(FTGAIterator iterator) {
        if (termLimit > 0) {
            if (sortStat >= 0) {
                try(final SilentCloseable ignored = TaskScheduler.CPUScheduler.lockSlot()) {
                    return FTGSIteratorUtil.getTopTermsFTGSIterator(iterator, termLimit, sortStat, sortOrder);
                }
            } else {
                return new TermLimitedFTGAIterator(iterator, termLimit);
            }
        } else {
            return iterator;
        }
    }
}

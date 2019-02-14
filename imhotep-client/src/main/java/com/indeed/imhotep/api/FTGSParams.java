package com.indeed.imhotep.api;

import javax.annotation.Nullable;
import java.util.List;

/**
 *  Class for getFTGSIterator method params
 */
public class FTGSParams {
    public final String[] intFields;
    public final String[] stringFields;
    public final long termLimit;
    public final int sortStat;
    public final boolean sorted;
    @Nullable
    public final List<List<String>> stats;

    /**
     * @param intFields list of int fields
     * @param stringFields list of string fields
     * @param termLimit - see {@link ImhotepSession#getFTGSIterator(FTGSParams)} for details
     * @param sortStat - see {@link ImhotepSession#getFTGSIterator(FTGSParams)} for details
     * @param sorted - see {@link ImhotepSession#getFTGSIterator(FTGSParams)} for details
     * @param stats - the stats to be returned for each group
     */
    public FTGSParams(
            final String[] intFields,
            final String[] stringFields,
            final long termLimit,
            final int sortStat,
            final boolean sorted,
            @Nullable final List<List<String>> stats
    ) {
        if ((intFields == null) || (stringFields == null)) {
            throw new IllegalArgumentException("Both int fields and strings must exist");
        }
        if (termLimit < 0) {
            throw new IllegalArgumentException("termLimit must be non-negative");
        }

        this.intFields = intFields;
        this.stringFields = stringFields;
        this.termLimit = termLimit;
        this.sortStat = sortStat;
        this.sorted = sorted;
        this.stats = stats;
    }

    public boolean isTopTerms() {
        return (sortStat >= 0) && (termLimit > 0);
    }

    public boolean isTermLimit() {
        return (sortStat < 0) && (termLimit > 0);
    }

    public FTGSParams copy() {
        return new FTGSParams(intFields, stringFields, termLimit, sortStat, sorted, stats);
    }

    public FTGSParams sortedCopy() {
        return new FTGSParams(intFields, stringFields, termLimit, sortStat, true, stats);
    }

    public FTGSParams unsortedCopy() {
        return new FTGSParams(intFields, stringFields, termLimit, sortStat, false, stats);
    }

    public FTGSParams unlimitedCopy() {
        return new FTGSParams(intFields, stringFields, 0, -1, sorted, stats);
    }
}

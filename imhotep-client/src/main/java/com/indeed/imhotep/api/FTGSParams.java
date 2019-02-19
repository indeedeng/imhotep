package com.indeed.imhotep.api;

import com.indeed.imhotep.protobuf.SortOrder;

/**
 *  Class for getFTGSIterator method params
 */
public class FTGSParams {
    public final String[] intFields;
    public final String[] stringFields;
    public final long termLimit;
    public final int sortStat;
    public final boolean sorted;
    public SortOrder sortOrder;

    /**
     * @param intFields list of int fields
     * @param stringFields list of string fields
     * @param termLimit - see {@link ImhotepSession#getFTGSIterator(FTGSParams)} for details
     * @param sortStat - see {@link ImhotepSession#getFTGSIterator(FTGSParams)} for details
     * @param sorted - see {@link ImhotepSession#getFTGSIterator(FTGSParams)} for details
     */
    public FTGSParams(
            final String[] intFields,
            final String[] stringFields,
            final long termLimit,
            final int sortStat,
            final boolean sorted,
            final SortOrder sortOrder
    ){
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
        this.sortOrder = sortOrder;
    }

    public boolean isTopTerms() {
        return (sortStat >= 0) && (termLimit > 0);
    }

    public boolean isTermLimit() {
        return (sortStat < 0) && (termLimit > 0);
    }

    public FTGSParams copy() {
        return new FTGSParams(intFields, stringFields, termLimit, sortStat, sorted, sortOrder);
    }

    public FTGSParams sortedCopy() {
        return new FTGSParams(intFields, stringFields, termLimit, sortStat, true, sortOrder);
    }

    public FTGSParams unsortedCopy() {
        return new FTGSParams(intFields, stringFields, termLimit, sortStat, false, sortOrder);
    }

    public FTGSParams unlimitedCopy() {
        return new FTGSParams(intFields, stringFields, 0, -1, sorted, sortOrder);
    }
}

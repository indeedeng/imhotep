package com.indeed.imhotep;

import com.indeed.imhotep.api.FTGSIterator;
import com.indeed.util.core.io.Closeables2;
import org.apache.log4j.Logger;

/**
 * Base class for merging FTGSIterators that have disjoint terms
 *
 * Each term exists only in one iterator and
 * after nextTerm() call iterators[0] is iterator with current term.
 */
public abstract class AbstractDisjointFTGSMerger implements FTGSIterator {
    private static final Logger log = Logger.getLogger(AbstractDisjointFTGSMerger.class);

    protected final FTGSIterator[] iterators;
    private final int numStats;
    private final int numGroups;

    // how many iterators in the beginning of array have terms for current field
    protected int numFieldIterators;
    protected boolean fieldIsIntType;

    private String fieldName;
    private boolean done = false;

    public AbstractDisjointFTGSMerger(final FTGSIterator[] iterators) {
        this.iterators = iterators;
        numStats = FTGSIteratorUtil.getNumStats(iterators);
        numGroups = FTGSIteratorUtil.getNumGroups(iterators);
    }

    public FTGSIterator[] getIterators() {
        return iterators;
    }

    @Override
    public int getNumStats() {
        return numStats;
    }

    @Override
    public int getNumGroups() {
        return numGroups;
    }

    @Override
    public boolean nextField() {
        if (done) {
            return false;
        }

        numFieldIterators = 0;

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
    public final  long termDocFreq() {
        return iterators[0].termDocFreq();
    }

    @Override
    public final long termIntVal() {
        return iterators[0].termIntVal();
    }

    @Override
    public final String termStringVal() {
        return iterators[0].termStringVal();
    }

    @Override
    public final byte[] termStringBytes() {
        return iterators[0].termStringBytes();
    }

    @Override
    public final int termStringLength() {
        return iterators[0].termStringLength();
    }

    @Override
    public final boolean nextGroup() {
        return iterators[0].nextGroup();
    }

    @Override
    public final int group() {
        return iterators[0].group();
    }

    @Override
    public final void groupStats(final long[] stats) {
        iterators[0].groupStats(stats);
    }

    @Override
    public final void close() {
        if (!done) {
            done = true;
            Closeables2.closeAll(log, iterators);
        }
    }
}
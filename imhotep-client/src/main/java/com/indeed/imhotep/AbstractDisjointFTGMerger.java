package com.indeed.imhotep;

import com.indeed.imhotep.api.FTGIterator;
import com.indeed.util.core.io.Closeables2;
import org.apache.log4j.Logger;

/**
 * Base class for merging FTGIterators that have disjoint terms
 *
 * Each term exists only in one iterator and
 * after nextTerm() call iterators[0] is iterator with current term.
 */
public abstract class AbstractDisjointFTGMerger<T extends FTGIterator> implements FTGIterator {
    private static final Logger log = Logger.getLogger(AbstractDisjointFTGMerger.class);

    protected final T[] iterators;
    private final int numGroups;

    // how many iterators in the beginning of array have terms for current field
    protected int numFieldIterators;
    protected boolean fieldIsIntType;

    private String fieldName;
    private boolean done = false;

    public AbstractDisjointFTGMerger(final T[] iterators) {
        this.iterators = iterators;
        numGroups = FTGSIteratorUtil.getNumGroups(iterators);
    }

    public T[] getIterators() {
        return iterators;
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

        final FTGIterator first = iterators[0];
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
            final FTGIterator itr = iterators[i];
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

    protected T currentIterator() {
        return iterators[0];
    }

    @Override
    public final void close() {
        if (!done) {
            done = true;
            Closeables2.closeAll(log, iterators);
        }
    }
}
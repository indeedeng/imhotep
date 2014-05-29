package com.indeed.imhotep;

import com.indeed.util.core.io.Closeables2;
import com.indeed.imhotep.api.FTGSIterator;
import org.apache.log4j.Logger;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.util.Collection;

/**
 * @author jsgroth
 */
public abstract class AbstractFTGSMerger implements FTGSIterator {
    private static final Logger log = Logger.getLogger(AbstractFTGSMerger.class);

    protected final FTGSIterator[] iterators;
    private final int numIterators;
    protected final int[] fieldIterators;
    protected int numFieldIterators;
    protected final int[] termIterators;
    protected final int[] termIteratorIndexes;
    protected int numTermIterators;
    protected int termIteratorsRemaining;

    private final long[] statAccumBuf;
    private final long[] statBuf;
    private final int numStats;

    private String fieldName;
    protected boolean fieldIsIntType;
    protected long termIntVal;
    private int group;

    private boolean done;

    private final Closeable doneCallback;

    protected AbstractFTGSMerger(Collection<? extends FTGSIterator> iterators, int numStats, @Nullable Closeable doneCallback) {
        this.doneCallback = doneCallback;
        numIterators = iterators.size();
        this.iterators = iterators.toArray(new FTGSIterator[numIterators]);
        fieldIterators = new int[numIterators];
        numFieldIterators = 0;
        termIterators = new int[numIterators];
        termIteratorIndexes = new int[numIterators];
        numTermIterators = 0;
        statAccumBuf = new long[numStats];
        statBuf = new long[numStats];
        this.numStats = numStats;
        done = false;
    }

    @Override
    public final boolean nextField() {
        if (done) return false;

        numFieldIterators = 0;

        final FTGSIterator first = iterators[0];
        if (!first.nextField()) {
            for (int i = 1; i < numIterators; ++i) {
                if (iterators[i].nextField()) {
                    throw new IllegalArgumentException("sub iterator fields do not match");
                }
            }
            close();
            return false;
        }
        fieldName = first.fieldName();
        fieldIsIntType = first.fieldIsIntType();
        numFieldIterators = 0;
        if (first.nextTerm()) {
            fieldIterators[numFieldIterators++] = 0;
        }

        for (int i = 1; i < numIterators; ++i) {
            final FTGSIterator itr = iterators[i];
            if (!itr.nextField() || !itr.fieldName().equals(fieldName) || itr.fieldIsIntType() != fieldIsIntType) {
                throw new IllegalArgumentException("sub iterator fields do not match");
            }
            if (itr.nextTerm()) {
                fieldIterators[numFieldIterators++] = i;
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
    public final long termDocFreq() {
        long ret = 0L;
        for (int i = 0; i < termIteratorsRemaining; ++i) {
            ret += iterators[termIterators[i]].termDocFreq();
        }
        return ret;
    }

    @Override
    public final long termIntVal() {
        return termIntVal;
    }

    @Override
    public final boolean nextGroup() {
        if (termIteratorsRemaining == 0) return false;

        int min = Integer.MAX_VALUE;
        for (int i = 0; i < termIteratorsRemaining; ++i) {
            final FTGSIterator itr = iterators[termIterators[i]];
            final int group = itr.group();
            if (group < min) {
                min = group;
            }
        }
        group = min;

        fill(statAccumBuf, 0, numStats, 0L);
        for (int i = 0; i < termIteratorsRemaining; ++i) {
            final FTGSIterator itr = iterators[termIterators[i]];
            if (itr.group() == min) {
                itr.groupStats(statBuf);
                for (int j = 0; j < numStats; ++j) {
                    statAccumBuf[j] += statBuf[j];
                }
                if (!itr.nextGroup()) {
                    swap(termIterators, i, --termIteratorsRemaining);
                    swap(termIteratorIndexes, i, termIteratorsRemaining);
                    --i;
                }
            }
        }

        return true;
    }

    @Override
    public final int group() {
        return group;
    }

    @Override
    public final void groupStats(long[] stats) {
        System.arraycopy(statAccumBuf, 0, stats, 0, numStats);
    }

    @Override
    public synchronized void close() {
        if (!done) {
            done = true;
            Closeables2.closeAll(log, Closeables2.forArray(log, iterators), doneCallback);
        }
    }

    protected static void swap(final int[] a, final int b, final int e) {
        final int t = a[b];
        a[b] = a[e];
        a[e] = t;
    }

    private static void fill(final long[] a, final int b, final int e, final long v) {
        for (int i = b; i < e; ++i)
            a[i] = v;
    }
}

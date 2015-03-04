package com.indeed.flamdex.simple;

import com.indeed.util.core.datastruct.IteratorMultiHeap;
import com.indeed.util.core.io.Closeables2;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Arrays;

/**
 * @author arun.
 */
abstract class AbstractMultiShardTermIterator<ITERATOR extends SimpleTermIterator> implements MultiShardTermIterator {
    private static final Logger log = Logger.getLogger(AbstractMultiShardTermIterator.class);
    private final ITERATOR[] iterators;
    protected long[] offsets;
    private final IteratorMultiHeap<IteratorIdPair> termStreamMerger;

    protected AbstractMultiShardTermIterator(final ITERATOR[] iterators) {
        this.iterators = Arrays.copyOf(iterators, iterators.length);
        this.termStreamMerger = new IteratorMultiHeap<IteratorIdPair>(iterators.length, IteratorIdPair.class) {

            @Override
            protected boolean next(IteratorIdPair iteratorIdPair) {
                return iteratorIdPair.simpleTermIterator.next();
            }

            @Override
            protected int compare(IteratorIdPair a, IteratorIdPair b) {
                return AbstractMultiShardTermIterator.this.compare(iterators[a.id], iterators[b.id]);
            }
        };
        for (int i = 0; i < iterators.length; i++) {
            termStreamMerger.add(new IteratorIdPair(iterators[i], i));
        }
        offsets = new long[iterators.length];
    }

    @Override
    public boolean next() {
        final boolean next = termStreamMerger.next();
        if (next) {
            Arrays.fill(offsets, -1);
            final IteratorIdPair[] iteratorIdPairs = termStreamMerger.getMin();
            recordTerm(iterators[iteratorIdPairs[0].id]);
            for (int i = 0; i < termStreamMerger.getMinLength(); i++) {
                final IteratorIdPair iteratorIdPair = iteratorIdPairs[i];
                offsets[iteratorIdPair.id] = iteratorIdPair.simpleTermIterator.getOffset();
            }
        }
        return next;
    }

    @Override
    public void offsets(long[] buffer) {
        System.arraycopy(offsets, 0, buffer, 0, offsets.length);
    }

    @Override
    public void close() throws IOException {
        Closeables2.closeAll(Arrays.asList(iterators), log);
    }

    /**
     * This is called when the iterator is advanced. Extending classes can implement this method to get read the
     * current value
     *
     * @param termIterator positioned term iterator
     */
    protected abstract void recordTerm(ITERATOR termIterator);

    /**
     * Extending classes are required to specify how to compare ITERATORs
     */
    protected abstract int compare(ITERATOR a, ITERATOR b);
}

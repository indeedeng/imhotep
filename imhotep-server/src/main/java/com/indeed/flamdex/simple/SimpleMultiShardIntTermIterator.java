package com.indeed.flamdex.simple;

import com.indeed.util.core.datastruct.IteratorMultiHeap;
import com.indeed.util.core.io.Closeables2;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Arrays;

/**
 * @author arun.
 */
final class SimpleMultiShardIntTermIterator implements MultiShardIntTermIterator {
    private static final Logger log = Logger.getLogger(SimpleMultiShardIntTermIterator.class);
    private static final class IteratorIdPair {
        private final int shardId;
        private final SimpleIntTermIterator intTermIterator;

        IteratorIdPair(SimpleIntTermIterator intTermIterator, int shardId) {
            this.intTermIterator = intTermIterator;
            this.shardId = shardId;
        }
    }

    private final SimpleIntTermIterator[] shardIntTerms;//one int term iterator per shard
    private final IteratorMultiHeap<IteratorIdPair> termStreamMerger;
    private final long[] offsets;
    private final int[] shardIds;
    private int shardCount;
    private long intTerm;

    SimpleMultiShardIntTermIterator(SimpleIntTermIterator[] iterators, int[] ids) {
        shardIntTerms = Arrays.copyOf(iterators, iterators.length);
        termStreamMerger = new IteratorMultiHeap<IteratorIdPair>(iterators.length, IteratorIdPair.class) {
            @Override
            protected boolean next(IteratorIdPair iteratorIdPair) {
                return iteratorIdPair.intTermIterator.next();
            }

            @Override
            protected int compare(IteratorIdPair a, IteratorIdPair b) {
                return Long.compare(a.intTermIterator.term(), b.intTermIterator.term());
            }
        };

        for (int i = 0; i < iterators.length; i++) {
            termStreamMerger.add(new IteratorIdPair(iterators[i], ids[i]));
        }

        offsets = new long[iterators.length];
        shardIds = new int[iterators.length];
    }

    @Override
    public boolean next() {
        final boolean next = termStreamMerger.next();
        if (next) {
            //the iterators of all the objects in minIteratorIdPairs point to the same term and every term in termStreamMerger
            //now is greater than the term the iterators point to.
            final IteratorIdPair[] minIteratorIdPairs = termStreamMerger.getMin();
            this.shardCount = termStreamMerger.getMinLength();
            for (int i = 0; i < shardCount; i++) {
                final SimpleIntTermIterator intTermIterator = minIteratorIdPairs[i].intTermIterator;
                offsets[i] = intTermIterator.getOffset();
                shardIds[i] = minIteratorIdPairs[i].shardId;
            }
            intTerm = minIteratorIdPairs[0].intTermIterator.term();
        }
        return next;
    }

    @Override
    public int offsets(long[] buffer) {
        System.arraycopy(offsets, 0, buffer, 0, shardCount);
        return shardCount;
    }

    @Override
    public int shardIds(int[] buffer) {
        System.arraycopy(shardIds, 0, buffer, 0, shardCount);
        return shardCount;
    }

    @Override
    public void close() throws IOException {
        Closeables2.closeAll(Arrays.asList(shardIntTerms), log);
    }

    @Override
    public long term() {
        return intTerm;
    }
}

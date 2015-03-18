package com.indeed.flamdex.simple;

import com.indeed.imhotep.multicache.ftgs.TermDesc;
import com.indeed.util.core.datastruct.IteratorMultiHeap;
import com.indeed.util.core.io.Closeables2;
import org.apache.log4j.Logger;

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;

/**
 * @author arun.
 */
final class SimpleMultiShardIntTermIterator implements Iterator<TermDesc>, Closeable {
    private static final Logger log = Logger.getLogger(SimpleMultiShardIntTermIterator.class);
    private final SimpleIntTermIterator[] shardIntTerms;//one int term iterator per shard
    private final IteratorMultiHeap<IteratorIdPair> termStreamMerger;
    private TermDesc nextTerm;
    private boolean hasMore;
    private final String fieldName;

    SimpleMultiShardIntTermIterator(String field, SimpleIntTermIterator[] iterators, int[] ids) {
        this.fieldName = field;
        this.shardIntTerms = Arrays.copyOf(iterators, iterators.length);
        this.termStreamMerger = new IteratorMultiHeap<IteratorIdPair>(iterators.length,
                                                                 IteratorIdPair.class) {
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

        this.nextTerm = nextTermDesc();
    }

    @Override
    public boolean hasNext() {
        return hasMore;
    }

    @Override
    public TermDesc next() {
        final TermDesc currentTerm = this.nextTerm;
        this.nextTerm = nextTermDesc();
        return currentTerm;
    }

    private TermDesc nextTermDesc() {
        if (!termStreamMerger.next()) {
            this.hasMore = false;
            return null;
        }
        this.hasMore = true;

        //the iterators of all the objects in minIteratorIdPairs point to the same term and
        // every term in termStreamMerger now is greater than the term the iterators point to.
        final IteratorIdPair[] minIteratorIdPairs = termStreamMerger.getMin();
        final int shardCount = termStreamMerger.getMinLength();
        final TermDesc result = new TermDesc(shardCount);

        for (int i = 0; i < shardCount; i++) {
            final SimpleIntTermIterator intTermIterator = minIteratorIdPairs[i].intTermIterator;
            result.offsets[i] = intTermIterator.getOffset();
            result.shardIds[i] = minIteratorIdPairs[i].shardId;
            result.numDocsInTerm[i] = intTermIterator.docFreq();
        }
        result.intTerm = minIteratorIdPairs[0].intTermIterator.term();
        result.isIntTerm = true;
        result.stringTerm = null;
        result.field = this.fieldName;
        return result;
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close() throws IOException {
        Closeables2.closeAll(Arrays.asList(shardIntTerms), log);
    }

    private static final class IteratorIdPair {
        private final int shardId;
        private final SimpleIntTermIterator intTermIterator;

        IteratorIdPair(SimpleIntTermIterator intTermIterator, int shardId) {
            this.intTermIterator = intTermIterator;
            this.shardId = shardId;
        }
    }

}

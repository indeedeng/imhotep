package com.indeed.flamdex.simple;

import com.indeed.flamdex.api.IntTermIterator;
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
    private final IntTermIterator[] shardIntTerms;//one int term iterator per shard
    private final IteratorMultiHeap<IteratorIdPair> termStreamMerger;
    private TermDesc nextTerm;
    private boolean hasMore;
    private final String fieldName;
    private final int numShards;

    SimpleMultiShardIntTermIterator(String field, IntTermIterator[] iterators) throws IOException {
        this.fieldName = field;
        this.numShards = iterators.length;
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
            final IntTermIterator iter = iterators[i];
            final SimpleIntTermIterator simpleIter;
            if (!(iter instanceof SimpleTermIterator)) {
                throw new IllegalArgumentException("invalid term iterator");
            }
            simpleIter = (SimpleIntTermIterator)iter;
            termStreamMerger.add(new IteratorIdPair(simpleIter, i));
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
        try {
            this.nextTerm = nextTermDesc();
        } catch (IOException e) {
            // Should never happen here - in the constructor maybe
            throw new RuntimeException(e);
        }
        return currentTerm;
    }

    private TermDesc nextTermDesc() throws IOException {
        if (!termStreamMerger.next()) {
            this.hasMore = false;
            return null;
        }
        this.hasMore = true;

        //the iterators of all the objects in minIteratorIdPairs point to the same term and
        // every term in termStreamMerger now is greater than the term the iterators point to.
        final IteratorIdPair[] minIteratorIdPairs = termStreamMerger.getMin();
        final int shardCount = termStreamMerger.getMinLength();
        final TermDesc result = new TermDesc(numShards);

        for (int i = 0; i < shardCount; i++) {
            final SimpleIntTermIterator intTermIterator = minIteratorIdPairs[i].intTermIterator;
            final int id = minIteratorIdPairs[i].iterNum;
            result.nativeDocAddresses[id] = intTermIterator.getOffset() + intTermIterator.getDocListAddress();
            result.numDocsInTerm[id] = intTermIterator.docFreq();
        }
        result.validShardCount = shardCount;
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
        private final int iterNum;
        private final SimpleIntTermIterator intTermIterator;

        IteratorIdPair(SimpleIntTermIterator intTermIterator, int iterNum) {
            this.intTermIterator = intTermIterator;
            this.iterNum = iterNum;
        }
    }

}

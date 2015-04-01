package com.indeed.flamdex.simple;

import com.indeed.flamdex.api.StringTermIterator;
import com.indeed.imhotep.RawFTGSMerger;
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
final class SimpleMultiShardStringTermIterator implements Iterator<TermDesc>, Closeable {
    private static final Logger log = Logger.getLogger(SimpleMultiShardStringTermIterator.class);
    private final StringTermIterator[] shardStringIters;//one int term iterator per shard
    private final IteratorMultiHeap<IteratorIdPair> termStreamMerger;
    private TermDesc nextTerm;
    private boolean hasMore;
    private final String fieldName;
    private final int numShards;

    SimpleMultiShardStringTermIterator(final String field,
                                       final StringTermIterator[] iterators) throws IOException {
        this.fieldName = field;
        this.numShards = iterators.length;
        this.shardStringIters = Arrays.copyOf(iterators, iterators.length);
        this.termStreamMerger = new IteratorMultiHeap<IteratorIdPair>(iterators.length,
                                                                      IteratorIdPair.class) {
            @Override
            protected boolean next(IteratorIdPair iteratorIdPair) {
                return iteratorIdPair.stringTermIterator.next();
            }

            @Override
            protected int compare(IteratorIdPair a, IteratorIdPair b) {
                return RawFTGSMerger.compareBytes(a.stringTermIterator.termStringBytes(),
                                                  a.stringTermIterator.termStringLength(),
                                                  b.stringTermIterator.termStringBytes(),
                                                  b.stringTermIterator.termStringLength());
            }
        };


        for (int i = 0; i < iterators.length; i++) {
            final StringTermIterator iter = iterators[i];
            final SimpleStringTermIterator simpleIter;
            if (!(iter instanceof SimpleTermIterator)) {
                throw new IllegalArgumentException("invalid term iterator");
            }
            simpleIter = (SimpleStringTermIterator) iter;
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
        final IteratorIdPair[] iteratorIdPairs = termStreamMerger.getMin();
        final int shardCount = termStreamMerger.getMinLength();
        final TermDesc result = new TermDesc(numShards);

        for (int i = 0; i < shardCount; i++) {
            final SimpleStringTermIterator stringTermIterator = iteratorIdPairs[i].stringTermIterator;
            final int iterNum = iteratorIdPairs[i].iterNum;
            result.nativeDocAddresses[iterNum] = stringTermIterator.getOffset() +
                    stringTermIterator.getDocListAddress();
            result.numDocsInTerm[iterNum] = stringTermIterator.docFreq();
        }
        result.size = shardCount;
        final int len = iteratorIdPairs[0].stringTermIterator.termStringLength();
        result.stringTerm = Arrays.copyOf(iteratorIdPairs[0].stringTermIterator.termStringBytes(),
                                          len);
        result.stringTermLen = len;
        result.isIntTerm = false;
        result.field = this.fieldName;
        return result;
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close() throws IOException {
        Closeables2.closeAll(Arrays.asList(shardStringIters), log);
    }

    private static final class IteratorIdPair {
        private final int iterNum;
        private final SimpleStringTermIterator stringTermIterator;

        IteratorIdPair(final SimpleStringTermIterator stringTermIterator, final int iterNum) {
            this.iterNum = iterNum;
            this.stringTermIterator = stringTermIterator;
        }
    }

}

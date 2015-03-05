package com.indeed.flamdex.simple;

import com.indeed.imhotep.RawFTGSMerger;
import com.indeed.util.core.datastruct.IteratorMultiHeap;
import com.indeed.util.core.io.Closeables2;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Arrays;

/**
 * @author arun.
 */
final class SimpleMultiShardStringTermIterator implements MultiShardStringTermIterator {
    private static final Logger log = Logger.getLogger(SimpleMultiShardStringTermIterator.class);
    private static final class IteratorIdPair {
        private final int shardId;
        private final SimpleStringTermIterator stringTermIterator;

        IteratorIdPair(final int shardId, final SimpleStringTermIterator stringTermIterator) {
            this.shardId = shardId;
            this.stringTermIterator = stringTermIterator;
        }
    }

    private final SimpleStringTermIterator[] shardStringTerms;//one int term iterator per shard
    private final IteratorMultiHeap<IteratorIdPair> termStreamMerger;
    private final long[] offsets;
    private byte[] rawStringTerm;
    private int rawStringTermLength;

    SimpleMultiShardStringTermIterator(final SimpleStringTermIterator[] stringTermIterators) {
        this.shardStringTerms = Arrays.copyOf(stringTermIterators, stringTermIterators.length);
        this.termStreamMerger = new IteratorMultiHeap<IteratorIdPair>(stringTermIterators.length, IteratorIdPair.class) {
            @Override
            protected boolean next(IteratorIdPair iteratorIdPair) {
                return iteratorIdPair.stringTermIterator.next();
            }

            @Override
            protected int compare(IteratorIdPair a, IteratorIdPair b) {
                return RawFTGSMerger.compareBytes(a.stringTermIterator.termStringBytes(),
                        a.stringTermIterator.termStringLength(),
                        b.stringTermIterator.termStringBytes(),
                        b.stringTermIterator.termStringLength()
                );
            }
        };
        offsets = new long[shardStringTerms.length];
    }

    @Override
    public boolean next() {
        final boolean next = termStreamMerger.next();
        if (next) {
            Arrays.fill(offsets, -1);
            final IteratorIdPair[] iteratorIdPairs = termStreamMerger.getMin();
            rawStringTerm = iteratorIdPairs[0].stringTermIterator.termStringBytes();
            rawStringTermLength = iteratorIdPairs[0].stringTermIterator.termStringLength();
            for (int i = 0; i < termStreamMerger.getMinLength(); i++) {
                offsets[iteratorIdPairs[i].shardId] = iteratorIdPairs[i].stringTermIterator.getOffset();
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
        Closeables2.closeAll(Arrays.asList(shardStringTerms), log);
    }

    @Override
    public byte[] termBytes() {
        return rawStringTerm;
    }

    @Override
    public int termBytesLength() {
        return rawStringTermLength;
    }
}

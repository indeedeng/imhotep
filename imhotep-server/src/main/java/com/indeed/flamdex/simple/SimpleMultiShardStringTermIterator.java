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

        IteratorIdPair(final SimpleStringTermIterator stringTermIterator, final int shardId) {
            this.shardId = shardId;
            this.stringTermIterator = stringTermIterator;
        }
    }

    private final SimpleStringTermIterator[] shardStringTerms;//one int term iterator per shard
    private final IteratorMultiHeap<IteratorIdPair> termStreamMerger;
    private final long[] offsets;
    private final int[] shardIds;
    private final int[] docFreqs;
    private int shardCount;
    private byte[] rawStringTerm;
    private int rawStringTermLength;

    SimpleMultiShardStringTermIterator(final SimpleStringTermIterator[] iterators, final int[] ids) {
        this.shardStringTerms = Arrays.copyOf(iterators, iterators.length);
        this.termStreamMerger = new IteratorMultiHeap<IteratorIdPair>(iterators.length, IteratorIdPair.class) {
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


        for (int i = 0; i < iterators.length; i++) {
            termStreamMerger.add(new IteratorIdPair(iterators[i], ids[i]));
        }

        offsets = new long[shardStringTerms.length];
        shardIds = new int[shardStringTerms.length];
        docFreqs = new int[shardStringTerms.length];
    }

    @Override
    public boolean next() {
        final boolean next = termStreamMerger.next();
        if (next) {
            final IteratorIdPair[] iteratorIdPairs = termStreamMerger.getMin();
            rawStringTerm = iteratorIdPairs[0].stringTermIterator.termStringBytes();
            rawStringTermLength = iteratorIdPairs[0].stringTermIterator.termStringLength();
            this.shardCount = termStreamMerger.getMinLength();
            for (int i = 0; i < shardCount; i++) {
                offsets[i] = iteratorIdPairs[i].stringTermIterator.getOffset();
                shardIds[i] = iteratorIdPairs[i].shardId;
                docFreqs[i] = iteratorIdPairs[i].stringTermIterator.docFreq();
            }
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
    public int docCounts(int[] buffer) {
        System.arraycopy(docFreqs, 0, buffer, 0, shardCount);
        return shardCount;
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

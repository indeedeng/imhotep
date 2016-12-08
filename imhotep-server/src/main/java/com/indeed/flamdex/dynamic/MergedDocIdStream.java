package com.indeed.flamdex.dynamic;

import com.indeed.flamdex.api.DocIdStream;
import com.indeed.flamdex.api.TermIterator;
import com.indeed.util.core.io.Closeables2;
import it.unimi.dsi.fastutil.ints.IntArrayFIFOQueue;
import org.apache.log4j.Logger;

import javax.annotation.Nonnull;
import java.util.List;

/**
 * {@link DocIdStream} that merges several {@link DocIdStream}.
 *
 * @author michihiko
 */
class MergedDocIdStream implements DocIdStream {
    private static final Logger LOG = Logger.getLogger(MergedDocIdStream.class);
    private final List<DocIdStream> docIdStreams;
    private final IntArrayFIFOQueue currentMinimums;
    private final int[] offsets;

    MergedDocIdStream(@Nonnull final List<DocIdStream> docIdStreams, @Nonnull final int[] offsets) {
        this.docIdStreams = docIdStreams;
        this.offsets = offsets;
        this.currentMinimums = new IntArrayFIFOQueue(docIdStreams.size());
    }

    @Override
    public void reset(final TermIterator term) {
        final MergedTermIterator mergedTermIterator = (MergedTermIterator) term;
        currentMinimums.clear();
        for (final int minimum : mergedTermIterator.getCurrentMinimums()) {
            docIdStreams.get(minimum).reset(mergedTermIterator.getInnerTermIterator(minimum));
            currentMinimums.enqueue(minimum);
        }
    }

    @Override
    public int fillDocIdBuffer(final int[] docIdBuffer) {
        if (currentMinimums.isEmpty()) {
            return 0;
        }
        int numFilled = docIdStreams.get(currentMinimums.firstInt()).fillDocIdBuffer(docIdBuffer);
        for (int i = 0; i < numFilled; ++i) {
            docIdBuffer[i] += offsets[currentMinimums.firstInt()];
        }
        while (numFilled < docIdBuffer.length) {
            currentMinimums.dequeueInt();
            if (currentMinimums.isEmpty()) {
                break;
            }
            final int required = docIdBuffer.length - numFilled;
            final int[] temporaryBuffer = new int[required];
            final int additional = docIdStreams.get(currentMinimums.firstInt()).fillDocIdBuffer(temporaryBuffer);
            for (int tmpIndex = 0; tmpIndex < additional; ++tmpIndex) {
                docIdBuffer[numFilled] = temporaryBuffer[tmpIndex] + offsets[currentMinimums.firstInt()];
                numFilled++;
            }
        }
        return numFilled;
    }

    @Override
    public void close() {
        Closeables2.closeAll(docIdStreams, LOG);
    }
}

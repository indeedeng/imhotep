package com.indeed.flamdex.dynamic;

import com.google.common.base.Preconditions;
import com.indeed.flamdex.api.DocIdStream;
import com.indeed.flamdex.api.TermIterator;
import com.indeed.util.core.io.Closeables2;
import org.apache.log4j.Logger;

import javax.annotation.Nonnull;
import java.util.BitSet;
import java.util.List;

/**
 * {@link DocIdStream} that merges several {@link DocIdStream}.
 * @author michihiko
 */
class MergedDocIdStream implements DocIdStream {
    private static final Logger LOG = Logger.getLogger(MergedDocIdStream.class);
    private final List<DocIdStream> docIdStreams;
    private final BitSet isMinimum;
    private int currentStream = 0;
    private final int[] offsets;

    MergedDocIdStream(@Nonnull final List<DocIdStream> docIdStreams, @Nonnull final int[] offsets) {
        this.docIdStreams = docIdStreams;
        this.offsets = offsets;
        this.isMinimum = new BitSet(docIdStreams.size());
    }

    @Override
    public void reset(final TermIterator term) {
        currentStream = 0;
        Preconditions.checkState(term instanceof MergedTermIterator);
        final MergedTermIterator mergedTermIterator = (MergedTermIterator) term;
        for (int i = 0; i < docIdStreams.size(); ++i) {
            docIdStreams.get(i).reset(mergedTermIterator.getInnerTermIterator(i));
        }
        isMinimum.clear();
        for (final int minimum : mergedTermIterator.getCurrentMinimums()) {
            isMinimum.set(minimum);
        }
    }

    @Override
    public int fillDocIdBuffer(final int[] docIdBuffer) {
        while ((currentStream < docIdStreams.size()) && !isMinimum.get(currentStream)) {
            ++currentStream;
        }
        if (currentStream == docIdStreams.size()) {
            return 0;
        }
        int numFilled = docIdStreams.get(currentStream).fillDocIdBuffer(docIdBuffer);
        for (int i = 0; i < numFilled; ++i) {
            docIdBuffer[i] += offsets[currentStream];
        }
        while (numFilled < docIdBuffer.length) {
            do {
                ++currentStream;
            } while ((currentStream < docIdStreams.size()) && !isMinimum.get(currentStream));
            if (currentStream == docIdStreams.size()) {
                break;
            }
            final int required = docIdBuffer.length - numFilled;
            final int[] temporaryBuffer = new int[required];
            final int additional = docIdStreams.get(currentStream).fillDocIdBuffer(temporaryBuffer);
            for (int tmpIndex = 0; tmpIndex < additional; ++tmpIndex) {
                docIdBuffer[numFilled] = temporaryBuffer[tmpIndex] + offsets[currentStream];
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

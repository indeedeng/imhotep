package com.indeed.imhotep.local;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.List;

import org.apache.log4j.Logger;

import com.indeed.util.core.Pair;
import com.indeed.flamdex.api.TermDocIterator;


public abstract class MergingTermDocIterator implements TermDocIterator {
    static final Logger log = Logger.getLogger(MergingTermDocIterator.class);

    protected final List<TermDocIterator> iters;
    protected final ArrayDeque<Pair<Integer, TermDocIterator>> itersAndOffsetsForTerm;
    protected int[] oldToNewDocIdMapping;
    protected List<Integer> iterNumToDocOffset;

    public MergingTermDocIterator(List<TermDocIterator> tdIters,
                                  int[] mapping,
                                  List<Integer> iterNumToDocOffset) {
        this.iters = tdIters;
        this.itersAndOffsetsForTerm = new ArrayDeque<Pair<Integer, TermDocIterator>>(iters.size());
        this.oldToNewDocIdMapping = mapping;
        this.iterNumToDocOffset = iterNumToDocOffset;
    }

    @Override
    public int fillDocIdBuffer(int[] docIdBuffer) {
        int len;
        int start;

        Pair<Integer, TermDocIterator> pair = itersAndOffsetsForTerm.peek();
        len = pair.getSecond().fillDocIdBuffer(docIdBuffer);
        renumberDocIds(pair.getFirst(), docIdBuffer, 0, len);
        start = 0;
        while (len + start < docIdBuffer.length) {
            /* read all the data from one iterator, try the next */
            itersAndOffsetsForTerm.pop();
            pair = itersAndOffsetsForTerm.peek();

            if (pair == null) {
                /* read all the data */
                return start + len;
            }

            /* copy the rest of the data we need into a new buffer */
            start += len;
            int[] tmpBuf = new int[docIdBuffer.length - start];
            len = pair.getSecond().fillDocIdBuffer(tmpBuf);
            for (int i = 0; i < len; i++) {
                docIdBuffer[i + start] = tmpBuf[i];
            }
            renumberDocIds(pair.getFirst(), docIdBuffer, start, start + len);
        }

        return docIdBuffer.length;
    }
    
    @Override
    public int docFreq() {
        return 0;
    }

    private void renumberDocIds(int offset, int[] docIdBuffer, int start, int end) {
        for (int i = start; i < end; ++i) {
            final int oldDocId = docIdBuffer[i];
            final int newDocId = oldToNewDocIdMapping[oldDocId + offset];
            docIdBuffer[i] = newDocId;
        }
    }
    
    public void close() {
        for (TermDocIterator iter : this.iters) {
            try {
                iter.close();
            } catch(IOException e) {
                log.error("Could not close TermDocIterator while optimizing index", e);
            }
        }
    }
}


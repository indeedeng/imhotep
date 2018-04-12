/*
 * Copyright (C) 2018 Indeed Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
 package com.indeed.imhotep.local;

import com.indeed.flamdex.api.TermDocIterator;
import com.indeed.util.core.Pair;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.List;


public abstract class MergingTermDocIterator implements TermDocIterator {
    static final Logger log = Logger.getLogger(MergingTermDocIterator.class);

    protected final List<TermDocIterator> iters;
    protected final ArrayDeque<Pair<Integer, TermDocIterator>> itersAndOffsetsForTerm;
    protected final int[] oldToNewDocIdMapping;
    protected final List<Integer> iterNumToDocOffset;

    public MergingTermDocIterator(final List<TermDocIterator> tdIters,
                                  final int[] mapping,
                                  final List<Integer> iterNumToDocOffset) {
        this.iters = tdIters;
        this.itersAndOffsetsForTerm = new ArrayDeque<>(iters.size());
        this.oldToNewDocIdMapping = mapping;
        this.iterNumToDocOffset = iterNumToDocOffset;
    }

    @Override
    public int fillDocIdBuffer(final int[] docIdBuffer) {
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
            final int[] tmpBuf = new int[docIdBuffer.length - start];
            len = pair.getSecond().fillDocIdBuffer(tmpBuf);
            System.arraycopy(tmpBuf, 0, docIdBuffer, start, len);
            renumberDocIds(pair.getFirst(), docIdBuffer, start, start + len);
        }

        return docIdBuffer.length;
    }
    
    @Override
    public int docFreq() {
        return 0;
    }

    private void renumberDocIds(final int offset, final int[] docIdBuffer, final int start, final int end) {
        for (int i = start; i < end; ++i) {
            final int oldDocId = docIdBuffer[i];
            final int newDocId = oldToNewDocIdMapping[oldDocId + offset];
            docIdBuffer[i] = newDocId;
        }
    }
    
    public void close() {
        for (final TermDocIterator iter : this.iters) {
            try {
                iter.close();
            } catch(final IOException e) {
                log.error("Could not close TermDocIterator while optimizing index", e);
            }
        }
    }
}


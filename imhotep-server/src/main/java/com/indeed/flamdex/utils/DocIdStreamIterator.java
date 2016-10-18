package com.indeed.flamdex.utils;

import com.indeed.flamdex.api.DocIdStream;
import com.indeed.flamdex.api.TermIterator;

import java.io.Closeable;

/**
 * @author zheli
 */
public class DocIdStreamIterator implements Closeable {
    private DocIdStream docIdStream;
    private int[] docIdBuffer;
    private int nextDocId;
    private int n;
    private int idx;
    private boolean isEnd;

    public DocIdStreamIterator(DocIdStream docIdStream, int bufferSize) {
        docIdBuffer = new int[bufferSize];
        this.docIdStream = docIdStream;
        nextDocId = -1;
        isEnd = true;
    }

    public void reset(TermIterator termIterator) {
        docIdStream.reset(termIterator);
        fillBuffer();
        nextDocId = readNext();
    }

    private int readNext() {
        if (idx == n && !isEnd) {
            fillBuffer();
        }
        if (idx < n) {
            int ret = docIdBuffer[idx];
            idx++;
            return ret;
        } else {
            return -1;
        }
    }

    private void fillBuffer() {
        idx = 0;
        n = docIdStream.fillDocIdBuffer(docIdBuffer);
        isEnd = n < docIdBuffer.length;
    }

    public boolean hasElement() {
        return nextDocId != -1;
    }

    public int docId() {
        return nextDocId;
    }

    public boolean advance() {
        nextDocId = readNext();
        return nextDocId != -1;
    }

    @Override
    public void close() {
        docIdStream.close();
    }
}


package com.indeed.flamdex.utils;

import com.google.common.collect.ImmutableList;
import com.indeed.flamdex.api.DocIdStream;
import com.indeed.flamdex.api.TermIterator;
import junit.framework.Assert;
import junit.framework.TestCase;

import java.util.List;

/**
 * @author zheli
 */
public class DocIdStreamIteratorTest extends TestCase{

    private class MockDocIdStream implements DocIdStream {
        private List<Integer> docs;
        private int index;

        public MockDocIdStream(List<Integer> docs) {
            this.docs = docs;
        }

        @Override
        public void reset(TermIterator term) {
            index = 0;
        }

        @Override
        public int fillDocIdBuffer(int[] docIdBuffer) {
            final int n = Math.min(docs.size() - index, docIdBuffer.length);
            for (int i = 0; i < n; ++i) {
                docIdBuffer[i] = docs.get(index++);
            }
            return n;
        }

        @Override
        public void close() {
        }
    }

    private void checkIterator(final DocIdStreamIterator docIdStreamIterator, final int n) {
        for (int i = 0; i < n; i++) {
            Assert.assertEquals(true, docIdStreamIterator.hasElement());
            Assert.assertEquals(i, docIdStreamIterator.docId());
            docIdStreamIterator.advance();
        }
    }

    public void testDocIdStreamIterator() throws Exception {
        final MockDocIdStream mockDocIdStream = new MockDocIdStream(ImmutableList.<Integer>of(0, 1, 2));
        final DocIdStreamIterator docIdStreamIterator = new DocIdStreamIterator(mockDocIdStream, 2);

        Assert.assertEquals(false, docIdStreamIterator.hasElement());
        Assert.assertEquals(-1, docIdStreamIterator.docId());

        docIdStreamIterator.reset(null);
        checkIterator(docIdStreamIterator, 2);
        Assert.assertEquals(true, docIdStreamIterator.hasElement());


        docIdStreamIterator.reset(null);
        checkIterator(docIdStreamIterator, 3);
        Assert.assertEquals(false, docIdStreamIterator.hasElement());
    }
}
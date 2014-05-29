package com.indeed.flamdex.utils;

import com.google.common.collect.Lists;
import com.indeed.flamdex.api.DocIdStream;
import com.indeed.flamdex.api.FlamdexReader;
import com.indeed.flamdex.api.IntTermIterator;
import com.indeed.flamdex.api.StringTermIterator;
import com.indeed.flamdex.writer.FlamdexDocument;

import java.util.List;

/**
 * @author jsgroth
 */
public class FlamdexReinverter {
    private FlamdexReinverter() {}

    public static List<FlamdexDocument> reinvertInMemory(final FlamdexReader r) {
        final List<FlamdexDocument> docs = Lists.newArrayListWithCapacity(r.getNumDocs());
        for (int i = 0; i < r.getNumDocs(); ++i) {
            docs.add(new FlamdexDocument());
        }

        final DocIdStream docIdStream = r.getDocIdStream();
        final int[] docIdBuffer = new int[64];
        try {
            for (final String intField : r.getIntFields()) {
                final IntTermIterator iter = r.getIntTermIterator(intField);
                try {
                    while (iter.next()) {
                        final long term = iter.term();
                        docIdStream.reset(iter);
                        while (true) {
                            final int n = docIdStream.fillDocIdBuffer(docIdBuffer);
                            for (int i = 0; i < n; ++i) {
                                docs.get(docIdBuffer[i]).addIntTerm(intField, term);
                            }
                            if (n < docIdBuffer.length) {
                                break;
                            }
                        }
                    }
                } finally {
                    iter.close();
                }
            }

            for (final String stringField : r.getStringFields()) {
                final StringTermIterator iter = r.getStringTermIterator(stringField);
                try {
                    while (iter.next()) {
                        final String term = iter.term();
                        docIdStream.reset(iter);
                        while (true) {
                            final int n = docIdStream.fillDocIdBuffer(docIdBuffer);
                            for (int i = 0; i < n; ++i) {
                                docs.get(docIdBuffer[i]).addStringTerm(stringField, term);
                            }
                            if (n < docIdBuffer.length) {
                                break;
                            }
                        }
                    }
                } finally {
                    iter.close();
                }
            }
        } finally {
            docIdStream.close();
        }

        return docs;
    }
}

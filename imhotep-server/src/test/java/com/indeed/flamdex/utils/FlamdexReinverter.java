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

        final int[] docIdBuffer = new int[64];
        try (DocIdStream docIdStream = r.getDocIdStream()) {
            for (final String intField : r.getIntFields()) {
                try (IntTermIterator iter = r.getUnsortedIntTermIterator(intField)) {
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
                }
            }

            for (final String stringField : r.getStringFields()) {
                try (StringTermIterator iter = r.getStringTermIterator(stringField)) {
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
                }
            }
        }

        return docs;
    }
}

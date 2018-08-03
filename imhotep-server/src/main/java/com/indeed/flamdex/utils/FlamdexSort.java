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

import com.indeed.flamdex.api.DocIdStream;
import com.indeed.flamdex.api.FlamdexReader;
import com.indeed.flamdex.api.IntTermIterator;
import com.indeed.flamdex.api.StringTermIterator;
import com.indeed.flamdex.writer.FlamdexWriter;
import com.indeed.flamdex.writer.IntFieldWriter;
import com.indeed.flamdex.writer.StringFieldWriter;
import com.indeed.util.core.sort.Quicksortables;
import com.indeed.util.core.sort.RadixSort;

import java.io.IOException;

/**
 * @author jsgroth
 */
public class FlamdexSort {
    private FlamdexSort() {
    }

    private static final int MAGIC_SORTING_NUMBER = 50000;    

    // this method DOES close the FlamdexWriter upon completion
    public static void sort(final FlamdexReader r, final FlamdexWriter w, final int[] oldDocIdToNewDocId) throws IOException {
        sort(r, w, oldDocIdToNewDocId, r.getIntFields(), r.getStringFields());
    }

    public static void sort(
            final FlamdexReader r,
            final FlamdexWriter w,
            final int[] oldDocIdToNewDocId,
            final Iterable<String> intFields,
            final Iterable<String> stringFields) throws IOException {
        final int[] docIdBuffer = new int[r.getNumDocs()];
        final int[] scratch = new int[r.getNumDocs()];
        final int[] countScratch = new int[65536]; // magic number

        try (final DocIdStream dis = r.getDocIdStream()) {

            for (final String intField : intFields) {
                System.out.println("intField:" + intField);
                try (final IntTermIterator iter = r.getIntTermIterator(intField);
                     final IntFieldWriter ifw = w.getIntFieldWriter(intField)) {
                    while (iter.next()) {
                        ifw.nextTerm(iter.term());
                        dis.reset(iter);
                        final int n = dis.fillDocIdBuffer(docIdBuffer);
                        for (int i = 0; i < n; ++i) {
                            docIdBuffer[i] = oldDocIdToNewDocId[docIdBuffer[i]];
                        }
                        if (n >= MAGIC_SORTING_NUMBER) {
                            RadixSort.radixSort(docIdBuffer, n, scratch, countScratch);
                        } else {
                            Quicksortables.sort(Quicksortables.getQuicksortableIntArray(docIdBuffer), n);
                        }
                        for (int i = 0; i < n; ++i) {
                            ifw.nextDoc(docIdBuffer[i]);
                        }
                    }
                }
            }

            for (final String stringField : stringFields) {
                System.out.println("stringField:" + stringField);
                try (final StringTermIterator iter = r.getStringTermIterator(stringField);
                     final StringFieldWriter sfw = w.getStringFieldWriter(stringField)) {
                    while (iter.next()) {
                        sfw.nextTerm(iter.term());
                        dis.reset(iter);
                        final int n = dis.fillDocIdBuffer(docIdBuffer);
                        for (int i = 0; i < n; ++i) {
                            docIdBuffer[i] = oldDocIdToNewDocId[docIdBuffer[i]];
                        }
                        if (n >= MAGIC_SORTING_NUMBER) {
                            RadixSort.radixSort(docIdBuffer, n, scratch, countScratch);
                        } else {
                            Quicksortables.sort(Quicksortables.getQuicksortableIntArray(docIdBuffer), n);
                        }
                        for (int i = 0; i < n; ++i) {
                            sfw.nextDoc(docIdBuffer[i]);
                        }
                    }
                }
            }
        }
        w.close();
    }
}

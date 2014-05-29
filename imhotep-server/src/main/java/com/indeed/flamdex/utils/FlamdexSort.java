package com.indeed.flamdex.utils;

import com.indeed.util.core.sort.Quicksortables;
import com.indeed.flamdex.api.DocIdStream;
import com.indeed.flamdex.api.FlamdexReader;
import com.indeed.flamdex.api.IntTermIterator;
import com.indeed.flamdex.api.StringTermIterator;
import com.indeed.flamdex.writer.FlamdexWriter;
import com.indeed.flamdex.writer.IntFieldWriter;
import com.indeed.flamdex.writer.StringFieldWriter;
import com.indeed.util.core.sort.RadixSort;

import java.io.IOException;

/**
 * @author jsgroth
 */
public class FlamdexSort {
    private static final int MAGIC_SORTING_NUMBER = 50000;    

    // this method DOES close the FlamdexWriter upon completion
    public static void sort(FlamdexReader r, FlamdexWriter w, int[] oldDocIdToNewDocId) throws IOException {
        sort(r, w, oldDocIdToNewDocId, r.getIntFields(), r.getStringFields());
    }

    public static void sort(FlamdexReader r, FlamdexWriter w, int[] oldDocIdToNewDocId, Iterable<String> intFields, Iterable<String> stringFields) throws IOException {
        final int[] docIdBuffer = new int[r.getNumDocs()];
        final int[] scratch = new int[r.getNumDocs()];
        final int[] countScratch = new int[65536]; // magic number

        final DocIdStream dis = r.getDocIdStream();

        for (final String intField : intFields) {
            System.out.println("intField:"+intField);
            final IntTermIterator iter = r.getIntTermIterator(intField);
            final IntFieldWriter ifw = w.getIntFieldWriter(intField);
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
            iter.close();
            ifw.close();
        }

        for (final String stringField : stringFields) {
            System.out.println("stringField:"+stringField);
            final StringTermIterator iter = r.getStringTermIterator(stringField);
            final StringFieldWriter sfw = w.getStringFieldWriter(stringField);
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
            iter.close();
            sfw.close();
        }

        dis.close();        
        w.close();
    }
}

/*
 * Copyright (C) 2014 Indeed Inc.
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
 package com.indeed.flamdex.simple;

import com.indeed.flamdex.api.DocIdStream;
import com.indeed.flamdex.api.FlamdexReader;
import com.indeed.flamdex.api.IntTermIterator;
import com.indeed.flamdex.api.StringTermIterator;
import com.indeed.flamdex.lucene.LuceneFlamdexReader;
import org.apache.lucene.index.IndexReader;

import java.io.IOException;
import java.util.Collection;

/**
 * @author jsgroth
 */
public class Benchmark {
    public static void main(String[] args) throws IOException, InterruptedException {
        final String luceneDir = args[0];
        final String simpleDir = args[1];

        final FlamdexReader reader1 = new LuceneFlamdexReader(IndexReader.open(luceneDir));
        final FlamdexReader reader2 = SimpleFlamdexReader.open(simpleDir);

        benchmark(reader1, reader2);
    }

    public static void benchmark(FlamdexReader reader1, FlamdexReader reader2) throws InterruptedException {
        final Collection<String> intFields = reader2.getIntFields();
//        final Collection<String> intFields = Collections.emptyList();
        final Collection<String> stringFields = reader2.getStringFields();
//        final Collection<String> stringFields = Collections.emptyList();

        for (int i = 0; i < 10; ++i) {
            System.gc();
            Thread.sleep(2000);
            doIt(reader1, intFields, stringFields);
        }

        for (int i = 0; i < 10; ++i) {
            System.gc();
            Thread.sleep(2000);
            doIt(reader2, intFields, stringFields);
        }
    }

    private static void doIt(final FlamdexReader reader, final Collection<String> intFields, final Collection<String> stringFields) {
        final int[] docIdBuf = new int[32];

        long elapsed = -System.currentTimeMillis();

        final DocIdStream docIdStream = reader.getDocIdStream();
        long someVar = 0L;
        for (final String intField : intFields) {
            final IntTermIterator it = reader.getIntTermIterator(intField);
            while (it.next()) {
                docIdStream.reset(it);
                while (true) {
                    final int n = docIdStream.fillDocIdBuffer(docIdBuf);
                    for (int i = 0; i < n; ++i) {
                        someVar += docIdBuf[i];
                    }
                    if (n < docIdBuf.length) break;
                }
            }
        }

        for (final String stringField : stringFields) {
            final StringTermIterator it = reader.getStringTermIterator(stringField);
            while (it.next()) {
                docIdStream.reset(it);
                while (true) {
                    final int n = docIdStream.fillDocIdBuffer(docIdBuf);
                    for (int i = 0; i < n; ++i) {
                        someVar += docIdBuf[i];
                    }
                    if (n < docIdBuf.length) break;
                }
            }
        }

        elapsed += System.currentTimeMillis();
        System.out.println("meaninglessVariable="+someVar);
        System.out.println("time for "+reader.getClass().getSimpleName()+":"+elapsed+"ms");
    }
}

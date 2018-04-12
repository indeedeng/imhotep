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

package com.indeed.flamdex.dynamic;

import com.google.common.collect.Ordering;
import com.indeed.flamdex.api.DocIdStream;
import com.indeed.flamdex.api.FlamdexOutOfMemoryException;
import com.indeed.flamdex.api.FlamdexReader;
import com.indeed.flamdex.api.IntTermIterator;
import com.indeed.flamdex.api.StringTermIterator;
import com.indeed.flamdex.query.Query;
import com.indeed.flamdex.query.Term;
import com.indeed.flamdex.writer.FlamdexDocWriter;
import com.indeed.flamdex.writer.FlamdexDocument;
import it.unimi.dsi.fastutil.longs.LongList;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author michihiko
 */

public class DynamicFlamdexTestUtils {
    private DynamicFlamdexTestUtils() {
    }

    @Nonnull
    public static FlamdexDocument makeDocument(final int original) {
        final FlamdexDocument.Builder builder = new FlamdexDocument.Builder();
        builder.addIntTerm("original", original);
        builder.addIntTerm("mod2i", original % 2);
        builder.addIntTerm("mod3i", original % 3);
        builder.addStringTerm("mod5s", Integer.toString(original % 5));
        builder.addIntTerms("mod7mod11i", original % 7, original % 11);
        builder.addStringTerms("mod7mod11s", Integer.toString(original % 7), Integer.toString(original % 11));
        if (((original % 3) != 0) && ((original % 5) != 0)) {
            builder.addIntTerms("mod3mod5i_nonzero", original % 3, original % 5);
        }
        return builder.build();
    }

    static int restoreOriginalNumber(@Nonnull final FlamdexReader flamdexReader, final int docId) throws FlamdexOutOfMemoryException {
        final long[] num = new long[1];
        flamdexReader.getMetric("original").lookup(new int[]{docId}, num, 1);
        assertTrue((Integer.MIN_VALUE <= num[0]) && (num[0] <= Integer.MAX_VALUE));
        return (int) num[0];
    }

    public static void addDocument(@Nonnull final Set<FlamdexDocument> naiveResult, @Nonnull final FlamdexDocWriter writer, @Nonnull final FlamdexDocument doc) throws IOException {
        writer.addDocument(doc);
        naiveResult.add(doc);
    }

    public static void removeDocument(@Nonnull final Set<FlamdexDocument> naiveResult, @Nonnull final DeletableFlamdexDocWriter writer, @Nonnull final String field, final long term) throws IOException {
        writer.deleteDocuments(Query.newTermQuery(Term.intTerm(field, term)));
        for (final Iterator<FlamdexDocument> iter = naiveResult.iterator(); iter.hasNext(); ) {
            final FlamdexDocument flamdexDocument = iter.next();
            final LongList terms = flamdexDocument.getIntTerms(field);
            if (terms == null) {
                continue;
            }
            for (final Long value : terms) {
                if (value == term) {
                    iter.remove();
                    break;
                }
            }
        }
    }

    private static long takeHashOfIndex(@Nonnull final FlamdexReader reader) {
        final int[] buf = new int[128];
        final Map<Integer, Integer> hashs = new HashMap<>();
        try (final DocIdStream docIdStream = reader.getDocIdStream()) {
            for (final String field : Ordering.natural().sortedCopy(reader.getIntFields())) {
                try (final IntTermIterator intTermIterator = reader.getIntTermIterator(field)) {
                    while (intTermIterator.next()) {
                        final long term = intTermIterator.term();
                        docIdStream.reset(intTermIterator);
                        while (true) {
                            final int num = docIdStream.fillDocIdBuffer(buf);
                            for (int i = 0; i < num; ++i) {
                                final int id = buf[i];
                                hashs.put(id, Objects.hash(hashs.containsKey(id) ? hashs.get(id) : 0, field, term));
                            }
                            if (num < buf.length) {
                                break;
                            }
                        }
                    }
                }
            }
            for (final String field : Ordering.natural().sortedCopy(reader.getStringFields())) {
                try (final StringTermIterator stringTermIterator = reader.getStringTermIterator(field)) {
                    while (stringTermIterator.next()) {
                        final String term = stringTermIterator.term();
                        docIdStream.reset(stringTermIterator);
                        while (true) {
                            final int num = docIdStream.fillDocIdBuffer(buf);
                            for (int i = 0; i < num; ++i) {
                                final int id = buf[i];
                                hashs.put(id, Objects.hash(hashs.containsKey(id) ? hashs.get(id) : 0, field, term));
                            }
                            if (num < buf.length) {
                                break;
                            }
                        }
                    }
                }
            }
        }
        return Arrays.hashCode(Ordering.natural().sortedCopy(hashs.values()).toArray());
    }

    private static long takeHashOfDocuments(@Nonnull final Set<FlamdexDocument> documents) {
        final List<Integer> hashs = new ArrayList<>(documents.size());
        for (final FlamdexDocument document : documents) {
            int hash = 0;
            for (final String field : Ordering.natural().sortedCopy(document.getIntFields().keySet())) {
                // sort and deduplicate terms.
                for (final Long term : new TreeSet<>(document.getIntTerms(field))) {
                    hash = Objects.hash(hash, field, term);
                }
            }
            for (final String field : Ordering.natural().sortedCopy(document.getStringFields().keySet())) {
                // sort and deduplicate terms.
                for (final String term : new TreeSet<>(document.getStringTerms(field))) {
                    hash = Objects.hash(hash, field, term);
                }
            }
            hashs.add(hash);
        }
        Collections.sort(hashs);
        return Arrays.hashCode(hashs.toArray());
    }

    public static void validateIndex(@Nonnull final Set<FlamdexDocument> naiveResult, @Nonnull final FlamdexReader reader) throws IOException {
        assertEquals(takeHashOfDocuments(naiveResult), takeHashOfIndex(reader));
    }
}

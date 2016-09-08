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
 package com.indeed.flamdex.reader;

import com.google.common.primitives.Ints;
import com.indeed.flamdex.api.DocIdStream;
import com.indeed.flamdex.api.FlamdexOutOfMemoryException;
import com.indeed.flamdex.api.FlamdexReader;
import com.indeed.flamdex.api.GenericIntTermDocIterator;
import com.indeed.flamdex.api.GenericStringTermDocIterator;
import com.indeed.flamdex.api.IntTermDocIterator;
import com.indeed.flamdex.api.IntTermIterator;
import com.indeed.flamdex.api.IntValueLookup;
import com.indeed.flamdex.api.StringTermDocIterator;
import com.indeed.flamdex.api.StringTermIterator;
import com.indeed.flamdex.api.StringValueLookup;
import com.indeed.flamdex.api.TermIterator;
import com.indeed.flamdex.utils.FlamdexUtils;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * @author jsgroth
 */
public class MockFlamdexReader implements FlamdexReader {
    private final Collection<String> intFields;
    private final Collection<String> stringFields;
    private final Collection<String> metrics;
    private final int numDocs;
    private Path directory;

    private final Map<String, SortedMap<Long, List<Integer>>> intTerms = new HashMap<String, SortedMap<Long, List<Integer>>>();
    private final Map<String, SortedMap<String, List<Integer>>> stringTerms = new HashMap<String, SortedMap<String, List<Integer>>>();

    public MockFlamdexReader() {
        this(Arrays.asList("if1"), Arrays.asList("sf1"), Arrays.asList("if1"), 10);
    }

    public MockFlamdexReader(Collection<String> intFields,
                             Collection<String> stringFields,
                             Collection<String> metrics,
                             int numDocs) {
        this.directory = Paths.get(".");
        this.intFields = intFields;
        this.stringFields = stringFields;
        this.metrics = metrics;
        this.numDocs = numDocs;

        for (final String intField : intFields) {
            intTerms.put(intField, new TreeMap<Long, List<Integer>>());
        }
        for (final String stringField : stringFields) {
            stringTerms.put(stringField, new TreeMap<String, List<Integer>>());
        }
    }

    public MockFlamdexReader(Collection<String> intFields,
                             Collection<String> stringFields,
                             Collection<String> metrics,
                             int numDocs,
                             Path directory) {
        this(intFields, stringFields, metrics, numDocs);
        this.directory = directory;
    }

    private class MockIntTermIterator implements IntTermIterator {
        private final Set<Map.Entry<Long, List<Integer>>> set;
        private Iterator<Map.Entry<Long, List<Integer>>> iterator;

        private long term;
        private List<Integer> docs;

        private boolean bufferNext;

        private MockIntTermIterator(final String field) {
            set = intTerms.get(field).entrySet();
            iterator = set.iterator();
        }

        @Override
        public void reset(long term) {
            iterator = set.iterator();
            while (iterator.hasNext()) {
                final Map.Entry<Long, List<Integer>> entry = iterator.next();
                if (entry.getKey() >= term) {
                    bufferNext = true;
                    this.term = entry.getKey();
                    this.docs = entry.getValue();
                    break;
                }
            }
        }

        @Override
        public long term() {
            return term;
        }

        @Override
        public boolean next() {
            if (bufferNext) {
                bufferNext = false;
                return true;
            }
            if (!iterator.hasNext()) return false;
            final Map.Entry<Long, List<Integer>> e = iterator.next();
            term = e.getKey();
            docs = e.getValue();
            return true;
        }

        @Override
        public int docFreq() {
            return docs.size();
        }

        @Override
        public void close() {
        }
    }

    private class MockStringTermIterator implements StringTermIterator {
        private final Set<Map.Entry<String, List<Integer>>> set;
        private Iterator<Map.Entry<String, List<Integer>>> iterator;

        private String term;
        private List<Integer> docs;

        private boolean bufferNext;

        private MockStringTermIterator(final String field) {
            set = stringTerms.get(field).entrySet();
            iterator = stringTerms.get(field).entrySet().iterator();
        }

        @Override
        public void reset(String term) {
            iterator = set.iterator();
            bufferNext = false;
            while (iterator.hasNext()) {
                final Map.Entry<String, List<Integer>> entry = iterator.next();
                if (entry.getKey().compareTo(term) >= 0) {
                    bufferNext = true;
                    this.term = entry.getKey();
                    this.docs = entry.getValue();
                    break;
                }
            }
        }

        @Override
        public String term() {
            return term;
        }

        @Override
        public boolean next() {
            if (bufferNext) {
                bufferNext = false;
                return true;
            }
            if (!iterator.hasNext()) return false;
            final Map.Entry<String, List<Integer>> e = iterator.next();
            term = e.getKey();
            docs = e.getValue();
            return true;
        }

        @Override
        public int docFreq() {
            return docs.size();
        }

        @Override
        public void close() {
        }
    }

    private abstract class AbstractMockEmptyIterator implements TermIterator {
        @Override
        public boolean next() {
            return false;
        }

        @Override
        public int docFreq() {
            return 0;
        }

        @Override
        public void close() {
        }
    }

    private class MockEmptyIntTermIterator extends AbstractMockEmptyIterator implements IntTermIterator {
        @Override
        public void reset(long term) {
        }

        @Override
        public long term() {
            return 0;
        }
    }

    private class MockEmptyStringTermIterator extends AbstractMockEmptyIterator implements StringTermIterator {
        @Override
        public void reset(String term) {
        }

        @Override
        public String term() {
            return null;
        }
    }

    private class MockDocIdStream implements DocIdStream {
        private List<Integer> docs;
        private int index;

        @Override
        public void reset(TermIterator term) {
            if (term instanceof MockIntTermIterator) {
                docs = ((MockIntTermIterator)term).docs;
            } else if (term instanceof MockStringTermIterator) {
                docs = ((MockStringTermIterator)term).docs;
            } else {
                throw new IllegalArgumentException("invalid term iterator: iterator is of type "+term.getClass().getName());
            }
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

    private class MockIntValueLookup implements IntValueLookup {
        private final long[] lookup;
        private final long min;
        private final long max;

        private MockIntValueLookup(String metric) {
            if (!intTerms.containsKey(metric)) {
                throw new IllegalArgumentException("don't have int field "+metric);
            }
            lookup = new long[numDocs];
            long tmin = Long.MAX_VALUE;
            long tmax = Long.MIN_VALUE;
            for (final Map.Entry<Long, List<Integer>> e : intTerms.get(metric).entrySet()) {
                final long term = e.getKey();
                tmin = Math.min(tmin, term);
                tmax = Math.max(tmax, term);
                final List<Integer> docs = e.getValue();
                for (final int doc : docs) {
                    lookup[doc] = term;
                }
            }
            min = tmin;
            max = tmax;
        }

        @Override
        public long getMin() {
            return min;
        }

        @Override
        public long getMax() {
            return max;
        }

        @Override
        public void lookup(int[] docIds, long[] values, int n) {
            for (int i = 0; i < n; ++i) {
                values[i] = lookup[docIds[i]];
            }
        }

        @Override
        public long memoryUsed() {
            return 0L;
        }

        @Override
        public void close() {
        }
    }

    private class MockStringValueLookup implements StringValueLookup {
        private final String[] lookup;

        private MockStringValueLookup(String metric) {
            if (!stringTerms.containsKey(metric)) {
                throw new IllegalArgumentException("don't have int field "+metric);
            }
            lookup = new String[numDocs];
            for (final Map.Entry<String, List<Integer>> e : stringTerms.get(metric).entrySet()) {
                final String term = e.getKey();
                final List<Integer> docs = e.getValue();
                for (final int doc : docs) {
                    lookup[doc] = term;
                }
            }
        }

        public String getString(final int docId) {
            return lookup[docId];
        }

        @Override
        public long memoryUsed() {
            return 0L;
        }

        @Override
        public void close() {
        }
    }

    @Override
    public Collection<String> getIntFields() {
        return intFields;
    }

    @Override
    public Collection<String> getStringFields() {
        return stringFields;
    }

    @Override
    public int getNumDocs() {
        return numDocs;
    }
    
    @Override
    public Path getDirectory() {
        return directory;
    }

    @Override
    public DocIdStream getDocIdStream() {
        return new MockDocIdStream();
    }

    @Override
    public IntTermIterator getIntTermIterator(String field) {
        return intTerms.containsKey(field) ? new MockIntTermIterator(field) : new MockEmptyIntTermIterator();
    }

    @Override
    public IntTermIterator getUnsortedIntTermIterator(String field) {
        return getIntTermIterator(field);
    }

    @Override
    public StringTermIterator getStringTermIterator(String field) {
        return stringTerms.containsKey(field) ? new MockStringTermIterator(field) : new MockEmptyStringTermIterator();
    }

    @Override
    public IntTermDocIterator getIntTermDocIterator(final String field) {
        return new GenericIntTermDocIterator(getIntTermIterator(field), getDocIdStream());
    }

    @Override
    public StringTermDocIterator getStringTermDocIterator(final String field) {
        return new GenericStringTermDocIterator(getStringTermIterator(field), getDocIdStream());
    }

    @Override
    public long getIntTotalDocFreq(String field) {
        return FlamdexUtils.getIntTotalDocFreq(this, field);
    }

    @Override
    public long getStringTotalDocFreq(String field) {
        return FlamdexUtils.getStringTotalDocFreq(this, field);
    }

    @Override
    public Collection<String> getAvailableMetrics() {
        return metrics;
    }

    @Override
    public IntValueLookup getMetric(String metric) throws FlamdexOutOfMemoryException {
        return new MockIntValueLookup(metric);
    }

    public StringValueLookup getStringLookup(final String field) throws FlamdexOutOfMemoryException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public long memoryRequired(String metric) {
        return 0L;
    }

    @Override
    public void close() throws IOException {
    }

    public void addIntTerm(final String field, final long term, final List<Integer> docs) {
        intTerms.get(field).put(term, docs);
    }

    public void addIntTerm(final String field, final long term, final int... docs) {
        addIntTerm(field, term, Ints.asList(docs));
    }

    public void addStringTerm(final String field, final String term, final List<Integer> docs) {
        stringTerms.get(field).put(term, docs);
    }

    public void addStringTerm(final String field, final String term, final int... docs) {
        addStringTerm(field, term, Ints.asList(docs));
    }
}

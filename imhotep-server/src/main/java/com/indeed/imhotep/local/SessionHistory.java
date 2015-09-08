/*
 * Copyright (C) 2015 Indeed Inc.
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

import com.indeed.flamdex.api.DocIdStream;
import com.indeed.flamdex.api.FlamdexOutOfMemoryException;
import com.indeed.flamdex.api.FlamdexReader;
import com.indeed.flamdex.api.IntTermDocIterator;
import com.indeed.flamdex.api.IntTermIterator;
import com.indeed.flamdex.api.IntValueLookup;
import com.indeed.flamdex.api.IntValueLookup;
import com.indeed.flamdex.api.StringTermDocIterator;
import com.indeed.flamdex.api.StringTermIterator;
import com.indeed.flamdex.api.StringValueLookup;
import com.indeed.imhotep.service.SessionHistoryIf;

import org.apache.log4j.Logger;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

public class SessionHistory implements SessionHistoryIf {

    private static final Logger log = Logger.getLogger(SessionHistory.class);

    /** Useful in test scenarios in which we just want to stub out SessionHistoryIf. */
    public static class Null implements SessionHistoryIf {
        public String getSessionId() { return ""; }
        public void onCreate(FlamdexReader reader) { }
        public void onGetFTGSIterator(String[] intFields, String[] stringFields) { }
        public void onWriteFTGSIteratorSplit(String[] intFields, String[] stringFields) { }
        public void onPushStat(String stat, IntValueLookup lookup) { }
        public FlamdexReader getFlamdexReader(FlamdexReader reader) { return reader; }
    }

    private final Map<String, FlamdexInfo.Abstract> flamdexInfoMap =
        new TreeMap<String, FlamdexInfo.Abstract>();

    private final Map<String, Integer> statsPushed =
        new TreeMap<String, Integer>();

    private final Set<String> datasetsUsed = new TreeSet<String>();
    private final Set<String> intFields    = new TreeSet<String>();
    private final Set<String> stringFields = new TreeSet<String>();
    private final Set<String> metrics      = new TreeSet<String>();

    private final String sessionId;

    public SessionHistory(final String sessionId) {
        this.sessionId = sessionId;
    }

    @Override
    public String getSessionId() { return sessionId; }

    @Override
    public void onCreate(FlamdexReader reader) {
        final String shardId = reader.getDirectory();
        if (!flamdexInfoMap.containsKey(shardId)) {
            flamdexInfoMap.put(shardId, FlamdexInfo.get(reader));
        }
    }

    @Override
    public void onGetFTGSIterator(String[] intFields, String[] stringFields) {
        this.intFields.addAll(Arrays.asList(intFields));
        this.stringFields.addAll(Arrays.asList(stringFields));
    }

    @Override
    public void onWriteFTGSIteratorSplit(String[] intFields, String[] stringFields) {
        this.intFields.addAll(Arrays.asList(intFields));
        this.stringFields.addAll(Arrays.asList(stringFields));
    }

    @Override
    public void onPushStat(String stat, IntValueLookup lookup) {
        final long    range        = lookup.getMax() - lookup.getMin();
        final int     leadingZeros = 64 - Long.numberOfLeadingZeros(range);
        final Integer sizeInBytes  = (leadingZeros + 7) / 8;

        final Integer current = statsPushed.get(stat);
        statsPushed.put(stat, current != null ?
                        Math.max(sizeInBytes, current) : sizeInBytes);
    }

    @Override
    public FlamdexReader getFlamdexReader(FlamdexReader reader) {
        return new FlamdexReaderWrapper(reader);
    }

    private class FlamdexReaderWrapper implements FlamdexReader {

        private final FlamdexReader wrapped;

        FlamdexReaderWrapper(FlamdexReader reader) { wrapped = reader; }

        private void    onIntField(String field)  { SessionHistory.this.intFields.add(field);    }
        private void onStringField(String field)  { SessionHistory.this.stringFields.add(field); }
        private void      onMetric(String metric) { SessionHistory.this.metrics.add(metric);     }

        public void close() throws java.io.IOException { wrapped.close(); }

        public Collection<String>    getIntFields() { return wrapped.getIntFields();    }
        public Collection<String> getStringFields() { return wrapped.getStringFields(); }

        public int             getNumDocs() { return wrapped.getNumDocs();     }
        public String        getDirectory() { return wrapped.getDirectory();   }
        public DocIdStream getDocIdStream() { return wrapped.getDocIdStream(); }

        public IntTermIterator getIntTermIterator(String field) {
            onIntField(field);
            return wrapped.getIntTermIterator(field);
        }

        public StringTermIterator getStringTermIterator(String field) {
            return wrapped.getStringTermIterator(field);
        }

        public IntTermDocIterator getIntTermDocIterator(String field) {
            onIntField(field);
            return wrapped.getIntTermDocIterator(field);
        }

        public StringTermDocIterator getStringTermDocIterator(String field) {
            return wrapped.getStringTermDocIterator(field);
        }

        public long getIntTotalDocFreq(String field) {
            onIntField(field);
            return wrapped.getIntTotalDocFreq(field);
        }

        public long getStringTotalDocFreq(String field) {
            onStringField(field);
            return wrapped.getStringTotalDocFreq(field);
        }

        public Collection<String> getAvailableMetrics() {
            return wrapped.getAvailableMetrics();
        }

        public IntValueLookup getMetric(String metric)
            throws FlamdexOutOfMemoryException {
            return wrapped.getMetric(metric);
        }

        public StringValueLookup getStringLookup(String field)
            throws FlamdexOutOfMemoryException {
            return wrapped.getStringLookup(field);
        }

        public long memoryRequired(String metric) {
            return wrapped.memoryRequired(metric);
        }
    }
}

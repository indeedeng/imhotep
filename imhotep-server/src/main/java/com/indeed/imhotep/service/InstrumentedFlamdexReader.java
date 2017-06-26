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

package com.indeed.imhotep.service;

import com.indeed.flamdex.api.DocIdStream;
import com.indeed.flamdex.api.FlamdexOutOfMemoryException;
import com.indeed.flamdex.api.FlamdexReader;
import com.indeed.flamdex.api.IntTermDocIterator;
import com.indeed.flamdex.api.IntTermIterator;
import com.indeed.flamdex.api.IntValueLookup;
import com.indeed.flamdex.api.StringTermDocIterator;
import com.indeed.flamdex.api.StringTermIterator;
import com.indeed.flamdex.api.StringValueLookup;
import com.indeed.imhotep.Instrumentation;
import com.indeed.imhotep.Instrumentation.Keys;
import it.unimi.dsi.fastutil.objects.Object2LongArrayMap;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Iterator;

public class InstrumentedFlamdexReader
    implements FlamdexReader {

    private static final Logger log = Logger.getLogger(InstrumentedFlamdexReader.class);

    private final FlamdexReader wrapped;
    private final FlamdexInfo   flamdexInfo;

    private final Object2LongArrayMap<String> statsPushed = new Object2LongArrayMap<String>(16);
    private final Object2LongArrayMap<String> fields      = new Object2LongArrayMap<String>(16);
    private final Object2LongArrayMap<String> metrics     = new Object2LongArrayMap<String>(16);

    final public class FlamdexReaderEvent extends Instrumentation.Event {
        FlamdexReaderEvent() {
            super(FlamdexReaderEvent.class.getSimpleName());
            getProperties().put(Keys.FIELDS,             commaDelimitted(fields.keySet()));
            getProperties().put(Keys.FIELD_BYTES,        commaDelimitted(fields.values()));
            getProperties().put(Keys.METRICS,            commaDelimitted(metrics.keySet()));
            getProperties().put(Keys.METRIC_BYTES,       commaDelimitted(metrics.values()));
            getProperties().put(Keys.SHARD_DATE,         flamdexInfo.getDate());
            getProperties().put(Keys.SHARD_ID,           flamdexInfo.getShardId());
            getProperties().put(Keys.SHARD_SIZE,         flamdexInfo.getSizeInBytes());
            getProperties().put(Keys.STATS_PUSHED,       commaDelimitted(statsPushed.keySet()));
            getProperties().put(Keys.STATS_PUSHED_BYTES, commaDelimitted(statsPushed.values()));
        }

        private <T> String commaDelimitted(Collection<T> items) {
            StringBuilder sb = new StringBuilder();
            Iterator<T>   it = items.iterator();
            while (it.hasNext()) {
                sb.append(it.next().toString());
                if (it.hasNext()) sb.append(", ");
            }
            return sb.toString();
        }
    }

    public InstrumentedFlamdexReader(FlamdexReader reader) {
        this.wrapped     = reader;
        this.flamdexInfo = new FlamdexInfo(reader);
    }

    public Instrumentation.Event sample() { return new FlamdexReaderEvent(); }

    public void onPushStat(String stat, IntValueLookup lookup) {
        if (stat != null) {
            statsPushed.put(stat, lookup != null ? lookup.memoryUsed() : 0L);
        }
    }

    private void onMetric(String metric) {
        metrics.put(metric, memoryRequired(metric));
    }

    private void onField(String field) {
        fields.put(field, fields.getLong(field));
    }

    @Override
    public void close() throws IOException { wrapped.close(); }

    public Collection<String>    getIntFields() { return wrapped.getIntFields();    }
    public Collection<String> getStringFields() { return wrapped.getStringFields(); }

    @Override
    public int getNumDocs() {
        return wrapped.getNumDocs();
    }

    @Override
    public Path getDirectory() {
        return wrapped.getDirectory();
    }

    @Override
    public DocIdStream getDocIdStream() {
        return wrapped.getDocIdStream();
    }

    public IntTermIterator getIntTermIterator(String field) {
        onField(field);
        return wrapped.getIntTermIterator(field);
    }

    public IntTermIterator getUnsortedIntTermIterator(String field) {
        onField(field);
        return wrapped.getUnsortedIntTermIterator(field);
    }

    public StringTermIterator getStringTermIterator(String field) {
        onField(field);
        return wrapped.getStringTermIterator(field);
    }

    public IntTermDocIterator getIntTermDocIterator(String field) {
        return wrapped.getIntTermDocIterator(field);
    }

    public StringTermDocIterator getStringTermDocIterator(String field) {
        return wrapped.getStringTermDocIterator(field);
    }

    public long getIntTotalDocFreq(String field) {
        return wrapped.getIntTotalDocFreq(field);
    }

    public long getStringTotalDocFreq(String field) {
        return wrapped.getStringTotalDocFreq(field);
    }

    public Collection<String> getAvailableMetrics() {
        return wrapped.getAvailableMetrics();
    }

    public IntValueLookup getMetric(String metric)
        throws FlamdexOutOfMemoryException {
        onMetric(metric);
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

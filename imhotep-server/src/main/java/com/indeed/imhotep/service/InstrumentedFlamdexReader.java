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
import com.indeed.flamdex.api.IntValueLookup;
import com.indeed.flamdex.api.StringTermDocIterator;
import com.indeed.flamdex.api.StringTermIterator;
import com.indeed.flamdex.api.StringValueLookup;
import com.indeed.imhotep.Instrumentation;
import com.indeed.imhotep.MemoryReservationContext;

import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

public class InstrumentedFlamdexReader
    implements FlamdexReader {

    private static final Logger log = Logger.getLogger(InstrumentedFlamdexReader.class);

    private final FlamdexReader wrapped;
    private final FlamdexInfo   flamdexInfo;

    private final Map<String, Integer> statsPushed = new TreeMap<String, Integer>();

    private final Set<String> intFields    = new TreeSet<String>();
    private final Set<String> stringFields = new TreeSet<String>();

    private final Map<String, Long> metricBytesRequired = new HashMap<String, Long>();

    final public class Event extends Instrumentation.Event {
        Event() {
            super(Event.class.getSimpleName());
            getProperties().put("shardId",     flamdexInfo.getShardId());
            getProperties().put("shardDate",   flamdexInfo.getDate());
            getProperties().put("shardSize",   flamdexInfo.getSizeInBytes());
            getProperties().put("statsPushed", statsPushed);
            getProperties().put("int-metrics", commaDelimitted(intFields));
            getProperties().put("int-metrics-bytes-required",
                                commaDelimitted(bytesRequired(intFields)));
            getProperties().put("string-fields", commaDelimitted(stringFields));
        }

        private String commaDelimitted(Collection<String> items) {
            StringBuilder    sb = new StringBuilder();
            Iterator<String> it = items.iterator();
            while (it.hasNext()) {
                sb.append(it.next());
                if (it.hasNext()) sb.append(", ");
            }
            return sb.toString();
        }

        private ArrayList<String> bytesRequired(Collection<String> metrics) {
            final ArrayList<String> result = new ArrayList<String>();
            Iterator<String> it = metrics.iterator();
            while (it.hasNext()) {
                final Long bytes = metricBytesRequired.get(it.next());
                result.add(bytes != null ? bytes.toString() : "(unknown)");
            }
            return result;
        }
    }

    public InstrumentedFlamdexReader(FlamdexReader reader) {
        this.wrapped     = reader;
        this.flamdexInfo = new FlamdexInfo(reader);
    }

    public Event sample() { return new Event(); }

    public void onPushStat(String stat, IntValueLookup lookup) {
        if (stat != null) {
            final long    range        = lookup != null ? lookup.getMax() - lookup.getMin() : 0;
            final int     leadingZeros = 64 - Long.numberOfLeadingZeros(range);
            final Integer sizeInBytes  = (leadingZeros + 7) / 8;

            final Integer current = statsPushed.get(stat);
            statsPushed.put(stat, current != null ?
                            Math.max(sizeInBytes, current) : sizeInBytes);
        }
    }

    private void onIntField(String field) {
        intFields.add(field);
        try {
            final Long bytesRequired = memoryRequired(field);
            metricBytesRequired.put(field, bytesRequired);
        }
        catch (Exception ex) {
            // Swallow this error. The upshot is that we just won't have a
            // bytesRequired value for this field in our CloseEvent.
        }
    }

    private void onStringField(String field) { stringFields.add(field); }

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
        onStringField(field);
        return wrapped.getStringTermIterator(field);
    }

    public IntTermDocIterator getIntTermDocIterator(String field) {
        onIntField(field);
        return wrapped.getIntTermDocIterator(field);
    }

    public StringTermDocIterator getStringTermDocIterator(String field) {
        onStringField(field);
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
        onIntField(metric);
        return wrapped.getMetric(metric);
    }

    public StringValueLookup getStringLookup(String field)
        throws FlamdexOutOfMemoryException {
        onStringField(field);
        return wrapped.getStringLookup(field);
    }

    public long memoryRequired(String metric) {
        return wrapped.memoryRequired(metric);
    }
}

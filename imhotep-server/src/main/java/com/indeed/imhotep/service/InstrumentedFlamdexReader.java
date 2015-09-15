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

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

public class InstrumentedFlamdexReader
    implements FlamdexReader, Instrumentation.Provider {

    private static final Logger log = Logger.getLogger(InstrumentedFlamdexReader.class);

    private final FlamdexReader wrapped;
    private final String        dataset;
    private final String        sessionId;
    private final String        username;
    private final FlamdexInfo   flamdexInfo;

    private final Map<String, Integer> statsPushed = new TreeMap<String, Integer>();

    private final Set<String> intFields    = new TreeSet<String>();
    private final Set<String> stringFields = new TreeSet<String>();
    private final Set<String> metrics      = new TreeSet<String>();

    private Instrumentation.ProviderSupport instrumentation =
        new Instrumentation.ProviderSupport();

    public static final String  OPEN_EVENT = OpenEvent.class.getSimpleName();
    public static final String CLOSE_EVENT = CloseEvent.class.getSimpleName();

    class Event extends Instrumentation.Event {
        protected Event(String name) {
            super(name);
            // !@# create common event property name constants somewhere...or use reflection?
            getProperties().put("dataset",     InstrumentedFlamdexReader.this.dataset);
            getProperties().put("shardId",     InstrumentedFlamdexReader.this.flamdexInfo.getShardId());
            getProperties().put("shardDate",   InstrumentedFlamdexReader.this.flamdexInfo.getDate());
            getProperties().put("shardSize",   InstrumentedFlamdexReader.this.flamdexInfo.getSizeInBytes());
            getProperties().put("sessionId",   InstrumentedFlamdexReader.this.sessionId);
            getProperties().put("statsPushed", InstrumentedFlamdexReader.this.statsPushed);
            getProperties().put("username",    InstrumentedFlamdexReader.this.username);
        }
    }

    final class  OpenEvent extends Event { public  OpenEvent() { super(OPEN_EVENT);  } }

    final class CloseEvent extends Event {
        public CloseEvent(long maxUsedMemory) {
            super(CLOSE_EVENT);
            getProperties().put("maxUsedMemory", maxUsedMemory);
        }
    }

    public InstrumentedFlamdexReader(FlamdexReader reader,
                                     String        dataset,
                                     String        sessionId,
                                     String        username) {
        this.wrapped     = reader;
        this.dataset     = dataset;
        this.sessionId   = sessionId;
        this.username    = username;
        this.flamdexInfo = new FlamdexInfo(reader);
    }

    public void addObserver(Instrumentation.Observer observer) {
        instrumentation.addObserver(observer);
    }

    public void removeObserver(Instrumentation.Observer observer) {
        instrumentation.removeObserver(observer);
    }

    public void onOpen() { instrumentation.fire(new OpenEvent());  }

    public void onClose(MemoryReservationContext mrc) {
        instrumentation.fire(new CloseEvent(mrc.maxUsedMemory()));
    }

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

    private void    onIntField(String field)  { intFields.add(field);    }
    private void onStringField(String field)  { stringFields.add(field); }
    private void      onMetric(String metric) { metrics.add(metric);     }

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

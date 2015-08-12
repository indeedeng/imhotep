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
 package com.indeed.imhotep.service;

import com.indeed.util.core.io.Closeables2;
import com.indeed.util.core.reference.SharedReference;
import com.indeed.flamdex.api.DocIdStream;
import com.indeed.flamdex.api.FlamdexOutOfMemoryException;
import com.indeed.flamdex.api.FlamdexReader;
import com.indeed.flamdex.api.IntTermDocIterator;
import com.indeed.flamdex.api.IntTermIterator;
import com.indeed.flamdex.api.IntValueLookup;
import com.indeed.flamdex.api.StringTermDocIterator;
import com.indeed.flamdex.api.StringTermIterator;
import com.indeed.flamdex.api.StringValueLookup;
import com.indeed.imhotep.ImhotepStatusDump;

import org.apache.log4j.Logger;

import java.util.Collection;
import java.util.List;
import java.util.Set;

/**
 * @author jplaisance
 */
public class CachedFlamdexReaderReference implements FlamdexReader, MetricCache {

    private static final Logger log = Logger.getLogger(CachedFlamdexReaderReference.class);

    private final SharedReference<? extends CachedFlamdexReader> reference;
    private final CachedFlamdexReader reader;
    private boolean closed = false;

    private final Exception constructorStackTrace;

    public CachedFlamdexReaderReference(final SharedReference<? extends CachedFlamdexReader> reference) {
        constructorStackTrace = new Exception();
        this.reference = reference;
        this.reader = reference.get();
    }

    @Override
    public Collection<String> getIntFields() {
        return reader.getIntFields();
    }

    @Override
    public Collection<String> getStringFields() {
        return reader.getStringFields();
    }

    @Override
    public int getNumDocs() {
        return reader.getNumDocs();
    }
    
    @Override
    public String getDirectory() {
        return reader.getDirectory();
    }

    @Override
    public DocIdStream getDocIdStream() {
        return reader.getDocIdStream();
    }

    @Override
    public IntTermIterator getIntTermIterator(final String field) {
        return reader.getIntTermIterator(field);
    }

    @Override
    public IntTermIterator getUnsortedIntTermIterator(final String field) {
        return reader.getUnsortedIntTermIterator(field);
    }

    @Override
    public StringTermIterator getStringTermIterator(final String field) {
        return reader.getStringTermIterator(field);
    }

    @Override
    public IntTermDocIterator getIntTermDocIterator(final String field) {
        return reader.getIntTermDocIterator(field);
    }

    @Override
    public StringTermDocIterator getStringTermDocIterator(final String field) {
        return reader.getStringTermDocIterator(field);
    }

    @Override
    public long getIntTotalDocFreq(final String field) {
        return reader.getIntTotalDocFreq(field);
    }

    @Override
    public long getStringTotalDocFreq(final String field) {
        return reader.getStringTotalDocFreq(field);
    }

    @Override
    public Collection<String> getAvailableMetrics() {
        return reader.getAvailableMetrics();
    }

    @Override
    public IntValueLookup getMetric(final String metric) throws FlamdexOutOfMemoryException {
        return reader.getMetric(metric);
    }

    public StringValueLookup getStringLookup(final String field) throws FlamdexOutOfMemoryException {
        return reader.getStringLookup(field);
    }

    @Override
    public long memoryRequired(final String metric) {
        return reader.memoryRequired(metric);
    }

    @Override
    public List<ImhotepStatusDump.MetricDump> getMetricDump() {
        return reader.getMetricDump();
    }

    @Override
    public Set<String> getLoadedMetrics() {
        return reader.getLoadedMetrics();
    }

    @Override
    public void close() {
        if (!closed) {
            closed = true;
            Closeables2.closeQuietly(reference, log);
        }
    }

    protected void finalize() throws Throwable {
        if (!closed) {
            log.error(getClass().getSimpleName()+" was not closed!!!!! stack trace at construction:", constructorStackTrace);
            close();
        }
    }
}

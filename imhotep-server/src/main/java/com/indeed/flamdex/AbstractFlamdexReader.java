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
 package com.indeed.flamdex;

import com.google.common.base.Throwables;
import com.google.common.collect.Maps;
import com.indeed.flamdex.api.FlamdexOutOfMemoryException;
import com.indeed.flamdex.api.FlamdexReader;
import com.indeed.flamdex.api.GenericIntTermDocIterator;
import com.indeed.flamdex.api.GenericStringTermDocIterator;
import com.indeed.flamdex.api.IntTermDocIterator;
import com.indeed.flamdex.api.IntValueLookup;
import com.indeed.flamdex.api.StringTermDocIterator;
import com.indeed.flamdex.api.StringValueLookup;
import com.indeed.flamdex.fieldcache.FieldCacher;
import com.indeed.flamdex.fieldcache.FieldCacherUtil;
import com.indeed.flamdex.fieldcache.UnsortedIntTermDocIterator;
import com.indeed.flamdex.fieldcache.UnsortedIntTermDocIteratorImpl;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;

/**
 * @author jsgroth
 *
 * FlamdexReader base class that provides implementations of some of the {@link FlamdexReader} methods
 *
 * at the moment of this comment's writing, {@link FlamdexReader#getMetric} and {@link FlamdexReader#memoryRequired} are implemented here
 */
public abstract class AbstractFlamdexReader implements FlamdexReader {
    protected final Path directory;
    protected int numDocs;
    protected final boolean useMMapMetrics;

    public static final class MinMax {
        public long min;
        public long max;
    }
    
    private final Map<String, FieldCacher> intFieldCachers;
    protected final Map<String, MinMax> metricMinMaxes;

    protected AbstractFlamdexReader(final Path directory, final int numDocs, final boolean useMMapMetrics) {
        this.directory = directory;
        this.numDocs = numDocs;
        this.useMMapMetrics = useMMapMetrics && directory != null;

        this.intFieldCachers = Maps.newHashMap();
        this.metricMinMaxes = Maps.newHashMap();
    }

    // this implementation will be correct for any FlamdexReader, but
    // subclasses may want to override for efficiency reasons
    protected UnsortedIntTermDocIterator createUnsortedIntTermDocIterator(final String field) {
        return UnsortedIntTermDocIteratorImpl.create(this, field);
    }

    @Override
    public IntValueLookup getMetric(final String metric) throws FlamdexOutOfMemoryException {
        final FieldCacher fieldCacher = getMetricCacher(metric);
        try (UnsortedIntTermDocIterator iterator = createUnsortedIntTermDocIterator(metric)) {
            return cacheField(iterator, metric, fieldCacher);
        }
    }

    public StringValueLookup getStringLookup(final String field) throws FlamdexOutOfMemoryException {
        try {
            return FieldCacherUtil.newStringValueLookup(field, this);
        } catch (final IOException e) {
            throw Throwables.propagate(e);
        }
    }

    private IntValueLookup cacheField(final UnsortedIntTermDocIterator iterator,
                                      final String metric,
                                      final FieldCacher fieldCacher) {
        final MinMax minMax = metricMinMaxes.get(metric);
        if (useMMapMetrics) {
            try {
                return fieldCacher.newMMapFieldCache(iterator,
                                                     numDocs,
                                                     metric,
                                                     directory,
                                                     minMax.min,
                                                     minMax.max);
            } catch (final IOException e) {
                throw new RuntimeException(e);
            }
        }
        return fieldCacher.newFieldCache(iterator, numDocs, minMax.min, minMax.max);
    }

    @Override
    public long memoryRequired(final String metric) {
        if (useMMapMetrics) {
            return 0;
        }

        final FieldCacher fieldCacher = getMetricCacher(metric);
        return fieldCacher.memoryRequired(numDocs);
    }

    private FieldCacher getMetricCacher(final String metric) {
        synchronized (intFieldCachers) {
            if (!intFieldCachers.containsKey(metric)) {
                final MinMax minMax = new MinMax();
                final FieldCacher cacher = FieldCacherUtil.getCacherForField(metric, this, minMax);
                intFieldCachers.put(metric, cacher);
                metricMinMaxes.put(metric, minMax);
            }
            return intFieldCachers.get(metric);
        }
    }

    @Override
    public IntTermDocIterator getIntTermDocIterator(final String field) {
        return new GenericIntTermDocIterator(getIntTermIterator(field), getDocIdStream());
    }

    @Override
    public StringTermDocIterator getStringTermDocIterator(final String field) {
        return new GenericStringTermDocIterator(getStringTermIterator(field), getDocIdStream());
    }

    protected void setNumDocs(final int nDocs) {
        this.numDocs = nDocs;
    }
    
    @Override
    public int getNumDocs() {
        return numDocs;
    }

    @Override
    public Path getDirectory() {
        return directory;
    }

}

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
import java.util.Map;

/**
 * @author jsgroth
 *
 * FlamdexReader base class that provides implementations of some of the {@link FlamdexReader} methods
 *
 * at the moment of this comment's writing, {@link FlamdexReader#getMetric} and {@link FlamdexReader#memoryRequired} are implemented here
 */
public abstract class AbstractFlamdexReader implements FlamdexReader {
    protected final String directory;
    protected final int numDocs;
    protected final boolean useMMapMetrics;

    public static final class MinMax {
        public long min;
        public long max;
    }
    
    private final Map<String, FieldCacher> intFieldCachers;
    protected final Map<String, MinMax> metricMinMaxes;

    protected AbstractFlamdexReader(String directory, int numDocs) {
        this(directory, numDocs, System.getProperty("flamdex.mmap.fieldcache") != null);
    }

    protected AbstractFlamdexReader(String directory, int numDocs, boolean useMMapMetrics) {
        this.directory = directory;
        this.numDocs = numDocs;
        this.useMMapMetrics = useMMapMetrics;

        this.intFieldCachers = Maps.newHashMap();
        this.metricMinMaxes = Maps.newHashMap();
    }

    // this implementation will be correct for any FlamdexReader, but
    // subclasses may want to override for efficiency reasons
    protected UnsortedIntTermDocIterator createUnsortedIntTermDocIterator(String field) {
        return UnsortedIntTermDocIteratorImpl.create(this, field);
    }

    @Override
    public IntValueLookup getMetric(String metric) throws FlamdexOutOfMemoryException {
        final FieldCacher fieldCacher = getMetricCacher(metric);
        final UnsortedIntTermDocIterator iterator = createUnsortedIntTermDocIterator(metric);
        try {
            return cacheField(iterator, metric, fieldCacher);
        } finally {
            iterator.close();
        }
    }

    public StringValueLookup getStringLookup(final String field) throws FlamdexOutOfMemoryException {
        try {
            return FieldCacherUtil.newStringValueLookup(field, this, directory);
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    private IntValueLookup cacheField(UnsortedIntTermDocIterator iterator, String metric, FieldCacher fieldCacher) {
        final MinMax minMax = metricMinMaxes.get(metric);
        if (useMMapMetrics) {
            try {
                return fieldCacher.newMMapFieldCache(iterator, numDocs, metric, directory, minMax.min, minMax.max);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        return fieldCacher.newFieldCache(iterator, numDocs, minMax.min, minMax.max);
    }

    @Override
    public long memoryRequired(String metric) {
        if (useMMapMetrics) return 0;

        final FieldCacher fieldCacher = getMetricCacher(metric);
        return fieldCacher.memoryRequired(numDocs);
    }

    private FieldCacher getMetricCacher(String metric) {
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
    
    @Override
    public int getNumDocs() {
        return numDocs;
    }

    @Override
    public String getDirectory() {
        return directory;
    }

}

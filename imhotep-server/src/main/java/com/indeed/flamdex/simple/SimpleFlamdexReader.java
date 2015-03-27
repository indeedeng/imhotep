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

import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.indeed.flamdex.AbstractFlamdexReader.MinMax;
import com.indeed.flamdex.api.FlamdexOutOfMemoryException;
import com.indeed.flamdex.api.IntValueLookup;
import com.indeed.flamdex.fieldcache.FieldCacher;
import com.indeed.flamdex.fieldcache.FieldCacherUtil;
import com.indeed.flamdex.fieldcache.NativeFlamdexFieldCacher;
import com.indeed.util.io.Files;
import com.indeed.flamdex.AbstractFlamdexReader;
import com.indeed.flamdex.api.DocIdStream;
import com.indeed.flamdex.api.GenericIntTermDocIterator;
import com.indeed.flamdex.api.GenericRawStringTermDocIterator;
import com.indeed.flamdex.api.IntTermDocIterator;
import com.indeed.flamdex.api.RawFlamdexReader;
import com.indeed.flamdex.api.RawStringTermDocIterator;
import com.indeed.flamdex.fieldcache.UnsortedIntTermDocIterator;
import com.indeed.flamdex.reader.FlamdexMetadata;
import com.indeed.flamdex.utils.FlamdexUtils;
import com.indeed.imhotep.io.caching.CachedFile;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author jsgroth
*/
public class SimpleFlamdexReader extends AbstractFlamdexReader implements RawFlamdexReader {
    private final Collection<String> intFields;
    private final Collection<String> stringFields;
    private final MapCache mapCache = new MapCache();

    private static final boolean useNativeDocIdStream;

    static {
        final String useNative = System.getProperties().getProperty("com.indeed.flamdex.simple.useNative");
        useNativeDocIdStream = "true".equalsIgnoreCase(useNative);
    }

    private final Map<String, NativeFlamdexFieldCacher> intFieldCachers;

    protected SimpleFlamdexReader(String directory,
                                  int numDocs,
                                  Collection<String> intFields,
                                  Collection<String> stringFields,
                                  boolean useMMapMetrics) {
        super(directory, numDocs, useMMapMetrics);

        this.intFields = intFields;
        this.stringFields = stringFields;
        this.intFieldCachers = Maps.newHashMap();
    }

    public static SimpleFlamdexReader open(String directory) throws IOException {
        return open(directory, new Config());
    }

    public static SimpleFlamdexReader open(String directory, Config config) throws IOException {
        final FlamdexMetadata metadata = FlamdexMetadata.readMetadata(directory);
        final Collection<String> intFields = scan(directory, ".intterms");
        final Collection<String> stringFields = scan(directory, ".strterms");
        if (config.writeBTreesIfNotExisting) {
            buildIntBTrees(directory, Lists.newArrayList(intFields));
            buildStringBTrees(directory, Lists.newArrayList(stringFields));
        }
        return new SimpleFlamdexReader(directory, metadata.numDocs, intFields, stringFields, config.useMMapMetrics);
    }

    protected static Collection<String> scan(final String directory, final String ending) throws IOException {
        final Set<String> fields = Sets.newTreeSet();
        final CachedFile dir = CachedFile.create(directory);
        
        for (final String name : dir.list()) {
            if (name.startsWith("fld-") && name.endsWith(ending)) {
                fields.add(name.substring(4, name.length() - ending.length()));
            }
        }

        return fields;
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
    public DocIdStream getDocIdStream() {
        return useNativeDocIdStream ? new NativeDocIdStream(mapCache) : new SimpleDocIdStream(mapCache);
    }

    @Override
    public SimpleIntTermIterator getIntTermIterator(String field) {
        final String termsFilename = CachedFile.buildPath(directory, SimpleIntFieldWriter.getTermsFilename(field));
        final String docsFilename = CachedFile.buildPath(directory, SimpleIntFieldWriter.getDocsFilename(field));
        if (CachedFile.create(termsFilename).length() == 0L) {
            return new NullIntTermIterator(docsFilename);
        }
        final String indexFilename = CachedFile.buildPath(directory, "fld-"+field);
        try {
            return new SimpleIntTermIteratorImpl(mapCache, termsFilename, docsFilename, indexFilename);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public SimpleStringTermIterator getStringTermIterator(String field) {
        final String termsFilename = CachedFile.buildPath(directory, SimpleStringFieldWriter.getTermsFilename(field));
        final String docsFilename = CachedFile.buildPath(directory, SimpleStringFieldWriter.getDocsFilename(field));
        if (CachedFile.create(termsFilename).length() == 0L) {
            return new NullStringTermIterator(docsFilename);
        }
        final String indexFilename = CachedFile.buildPath(directory, "fld-"+field+".strindex");
        try {
            return new SimpleStringTermIteratorImpl(mapCache, termsFilename, docsFilename, indexFilename);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected UnsortedIntTermDocIterator createUnsortedIntTermDocIterator(final String field) {
        return getIntTermDocIterator(field);
    }

    @Override
    public IntTermDocIterator getIntTermDocIterator(final String field) {
        final SimpleIntTermIterator termIterator = getIntTermIterator(field);
        if (useNativeDocIdStream && CachedFile.create(termIterator.getFilename()).length() > 0) {
            try {
                return new NativeIntTermDocIterator(termIterator, mapCache);
            } catch (IOException e) {
                throw Throwables.propagate(e);
            }
        } else {
            return new GenericIntTermDocIterator(termIterator, getDocIdStream());
        }
    }

    @Override
    public RawStringTermDocIterator getStringTermDocIterator(final String field) {
        final SimpleStringTermIterator termIterator = getStringTermIterator(field);
        if (useNativeDocIdStream && CachedFile.create(termIterator.getFilename()).length() > 0) {
            try {
                return new NativeStringTermDocIterator(termIterator, mapCache);
            } catch (IOException e) {
                throw Throwables.propagate(e);
            }
        } else {
            return new GenericRawStringTermDocIterator(termIterator, getDocIdStream());
        }
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
        return intFields;
    }

    @Override
    public final IntValueLookup getMetric(String metric) throws FlamdexOutOfMemoryException {
        final NativeFlamdexFieldCacher fieldCacher = getMetricCacher(metric);
        final SimpleIntTermIterator iterator = getIntTermIterator(metric);
        try {
            return cacheField(iterator, metric, fieldCacher);
        } finally {
            iterator.close();
        }
    }

    private IntValueLookup cacheField(SimpleIntTermIterator iterator,
                                      String metric,
                                      NativeFlamdexFieldCacher fieldCacher) {
        final MinMax minMax = metricMinMaxes.get(metric);
        try {
            return useMMapMetrics ?
                fieldCacher.newMMapFieldCache(iterator, numDocs, metric, directory, minMax.min, minMax.max) :
                fieldCacher.newFieldCache(iterator, numDocs, minMax.min, minMax.max);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public final long memoryRequired(String metric) {
        if (useMMapMetrics)
            return 0;

        final NativeFlamdexFieldCacher fieldCacher = getMetricCacher(metric);
        return fieldCacher.memoryRequired(numDocs);
    }

    private NativeFlamdexFieldCacher getMetricCacher(String metric) {
        synchronized (intFieldCachers) {
            if (!intFieldCachers.containsKey(metric)) {
                final MinMax minMax = new MinMax();
                final NativeFlamdexFieldCacher cacher =
                        FieldCacherUtil.getNativeCacherForField(metric, this, minMax);
                intFieldCachers.put(metric, cacher);
                metricMinMaxes.put(metric, minMax);
            }
            return intFieldCachers.get(metric);
        }
    }

    @Override
    public void close() throws IOException {
        mapCache.close();
    }

    protected static void buildIntBTrees(final String directory, final List<String> intFields) throws IOException {
        for (final String intField : intFields) {
            final File btreeDir = new File(Files.buildPath(directory, "fld-" + intField + ".intindex"));
            final File btreeDir64 = new File(Files.buildPath(directory, "fld-" + intField + ".intindex64"));
            if (!btreeDir.exists() && !btreeDir64.exists()) {
                SimpleFlamdexWriter.writeIntBTree(directory, intField, btreeDir64);
            }
        }
    }

    protected static void buildStringBTrees(final String directory, final List<String> stringFields) throws IOException {
        for (final String stringField : stringFields) {
            final File btreeDir = new File(Files.buildPath(directory, "fld-" + stringField + ".strindex"));
            if (!btreeDir.exists()) {
                SimpleFlamdexWriter.writeStringBTree(directory, stringField, btreeDir);
            }
        }
    }

    public static final class Config {
        private boolean writeBTreesIfNotExisting = true;
        private boolean useMMapMetrics = System.getProperty("flamdex.mmap.fieldcache") != null;

        public boolean isWriteBTreesIfNotExisting() {
            return writeBTreesIfNotExisting;
        }

        public Config setWriteBTreesIfNotExisting(boolean writeBTreesIfNotExisting) {
            this.writeBTreesIfNotExisting = writeBTreesIfNotExisting;
            return this;
        }

        public boolean isUseMMapMetrics() {
            return useMMapMetrics;
        }

        public Config setUseMMapMetrics(boolean useMMapMetrics) {
            this.useMMapMetrics = useMMapMetrics;
            return this;
        }
    }
}

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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.indeed.flamdex.AbstractFlamdexReader;
import com.indeed.flamdex.api.DocIdStream;
import com.indeed.flamdex.api.FieldsCardinalityMetadata;
import com.indeed.flamdex.api.FlamdexOutOfMemoryException;
import com.indeed.flamdex.api.GenericIntTermDocIterator;
import com.indeed.flamdex.api.GenericRawStringTermDocIterator;
import com.indeed.flamdex.api.IntTermDocIterator;
import com.indeed.flamdex.api.IntValueLookup;
import com.indeed.flamdex.api.RawFlamdexReader;
import com.indeed.flamdex.api.RawStringTermDocIterator;
import com.indeed.flamdex.fieldcache.FieldCacherUtil;
import com.indeed.flamdex.fieldcache.NativeFlamdexFieldCacher;
import com.indeed.flamdex.fieldcache.UnsortedIntTermDocIterator;
import com.indeed.flamdex.reader.FlamdexMetadata;
import com.indeed.flamdex.utils.FlamdexUtils;

import javax.annotation.Nonnull;
import java.io.File;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author jsgroth
 */
public class SimpleFlamdexReader
        extends AbstractFlamdexReader
        implements RawFlamdexReader {

    private final Collection<String> intFields;
    private final Collection<String> stringFields;
    private final MapCache mapCache = new MapCache();
    private FieldsCardinalityMetadata cardinalityMetadata;

    private static final boolean useNativeDocIdStream;
    private final boolean useMMapDocIdStream;

    static {
        final String useNative = System.getProperties().getProperty("com.indeed.flamdex.simple.useNative");
        useNativeDocIdStream = "true".equalsIgnoreCase(useNative);
    }

    private final Map<String, NativeFlamdexFieldCacher> intFieldCachers;

    protected SimpleFlamdexReader(final Path directory,
                                  final int numDocs,
                                  final Collection<String> intFields,
                                  final Collection<String> stringFields,
                                  final boolean useMMapMetrics) {
        this(directory, numDocs, intFields, stringFields, useMMapMetrics, true);
    }

    protected SimpleFlamdexReader(final Path directory,
                                  final int numDocs,
                                  final Collection<String> intFields,
                                  final Collection<String> stringFields,
                                  final boolean useMMapMetrics,
                                  final boolean useMMapDocIdStream) {
        super(directory, numDocs, useMMapMetrics);

        this.intFields = intFields;
        this.stringFields = stringFields;
        this.intFieldCachers = Maps.newHashMap();

        this.useMMapDocIdStream = useMMapDocIdStream;

        this.cardinalityMetadata = FieldsCardinalityMetadata.open(directory);
    }

    /**
     * Use {@link #open(Path)} instead
     */
    @Deprecated
    public static SimpleFlamdexReader open(final String directory) throws IOException {
        return open(new File(directory).toPath());
    }

    public static SimpleFlamdexReader open(@Nonnull final Path directory) throws IOException {
        return open(directory, new Config());
    }

    /**
     * Use {@link #open(Path, Config)} instead
     */
    @Deprecated
    public static SimpleFlamdexReader open(final String directory, final Config config) throws IOException {
        return open(new File(directory).toPath(), config);
    }

    public static SimpleFlamdexReader open(@Nonnull final Path directory, final Config config) throws IOException {
        final FlamdexMetadata metadata = FlamdexMetadata.readMetadata(directory);
        final Collection<String> intFields = scan(directory, ".intterms");
        final Collection<String> stringFields = scan(directory, ".strterms");
        if (config.isWriteBTreesIfNotExisting()) {
            buildIntBTrees(directory, Lists.newArrayList(intFields));
            buildStringBTrees(directory, Lists.newArrayList(stringFields));
        }
        final SimpleFlamdexReader result = new SimpleFlamdexReader(directory, metadata.getNumDocs(), intFields, stringFields,
                config.useMMapMetrics, config.useMMapDocIdStream);
        if (config.isWriteCardinalityIfNotExisting()) {
            // try to build and store cache
            result.buildAndWriteCardinalityCache(true);
        }
        return result;
    }

    protected static Collection<String> scan(final Path directory, final String ending) throws IOException {
        final Set<String> fields = Sets.newTreeSet();

        try (final DirectoryStream<Path> dirStream = Files.newDirectoryStream(directory)) {
            for (final Path file : dirStream) {
                final String name = file.getFileName().toString();
                if (name.startsWith("fld-") && name.endsWith(ending)) {
                    fields.add(name.substring(4, name.length() - ending.length()));
                }
            }
        }

        return fields;
    }

    public MapCache getMapCache() {
        return mapCache;
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
        if (useMMapDocIdStream) {
            return useNativeDocIdStream ? new NativeDocIdStream(mapCache) : new MMapDocIdStream(mapCache);
        } else {
            return new ByteChannelDocIdStream();
        }
    }

    @Override
    public SimpleIntTermIterator getUnsortedIntTermIterator(final String field) {
        return getIntTermIterator(field, true);
    }

    @Override
    public SimpleIntTermIterator getIntTermIterator(final String field) {
        return getIntTermIterator(field, false);
    }

    private SimpleIntTermIterator getIntTermIterator(final String field, final boolean unsorted) {
        final Path termsPath = directory.resolve(SimpleIntFieldWriter.getTermsFilename(field));
        final Path docsPath = directory.resolve(SimpleIntFieldWriter.getDocsFilename(field));

        try {
            if (Files.notExists(termsPath) || Files.size(termsPath) == 0L) {
                // try to read it as a String field and convert to ints
                final SimpleStringTermIterator stringTermIterator = getStringTermIterator(field);
                if (!(stringTermIterator instanceof NullStringTermIterator)) {
                    if (unsorted) {
                        return new UnsortedStringToIntTermIterator(stringTermIterator);
                    } else {
                        final Supplier<SimpleStringTermIterator> stringTermIteratorSupplier =
                                new Supplier<SimpleStringTermIterator>() {
                                    @Override
                                    public SimpleStringTermIterator get() {
                                        return getStringTermIterator(field);
                                    }
                                };
                        return new StringToIntTermIterator(stringTermIterator,
                                stringTermIteratorSupplier);
                    }
                }

                // string field not found. return a null iterator
                return new NullIntTermIterator(docsPath);
            }
            final Path indexPath;
            final Path intIndex = directory.resolve(SimpleIntFieldWriter.get32IndexFilename(field));
            final Path intIndex64 = directory.resolve(SimpleIntFieldWriter.get64IndexFilename(field));
            if (Files.exists(intIndex64)) {
                indexPath = intIndex64;
            } else if (Files.exists(intIndex)) {
                indexPath = intIndex;
            } else {
                indexPath = null;
            }
            return new SimpleIntTermIteratorImpl(mapCache, termsPath, docsPath, indexPath);
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public SimpleStringTermIterator getStringTermIterator(final String field) {
        final Path termsPath = directory.resolve(SimpleStringFieldWriter.getTermsFilename(field));
        final Path docsPath = directory.resolve(SimpleStringFieldWriter.getDocsFilename(field));

        try {
            if (Files.notExists(termsPath) || Files.size(termsPath) == 0L) {
                return new NullStringTermIterator(docsPath);
            }
            final Path indexPath = directory.resolve(SimpleStringFieldWriter.getIndexFilename(field));
            return new SimpleStringTermIteratorImpl(mapCache, termsPath, docsPath, indexPath);
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected UnsortedIntTermDocIterator createUnsortedIntTermDocIterator(final String field) {
        return getIntTermDocIterator(field, true);
    }

    @Override
    public IntTermDocIterator getIntTermDocIterator(final String field) {
        return getIntTermDocIterator(field, false);
    }

    public IntTermDocIterator getIntTermDocIterator(final String field, final boolean unsorted) {
        final SimpleIntTermIterator termIterator = getIntTermIterator(field, unsorted);
        final Path termsPath = termIterator.getFilename();
        // NativeIntTermDocIterator doesn't work reliably with reordering like StringToIntTermIterator
        try {
            if (useNativeDocIdStream && !(termIterator instanceof StringToIntTermIterator) &&
                    (Files.exists(termsPath) && (Files.size(termsPath) > 0))) {
                return new NativeIntTermDocIterator(termIterator, mapCache);
            } else {
                return new GenericIntTermDocIterator(termIterator, getDocIdStream());
            }
        } catch (final IOException e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public RawStringTermDocIterator getStringTermDocIterator(final String field) {
        final SimpleStringTermIterator termIterator = getStringTermIterator(field);
        final Path termsPath = termIterator.getFilename();

        try {
            if (useNativeDocIdStream && (Files.exists(termsPath) && (Files.size(termsPath) > 0))) {
                return new NativeStringTermDocIterator(termIterator, mapCache);
            } else {
                return new GenericRawStringTermDocIterator(termIterator, getDocIdStream());
            }
        } catch (final IOException e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public long getIntTotalDocFreq(final String field) {
        return FlamdexUtils.getIntTotalDocFreq(this, field);
    }

    @Override
    public long getStringTotalDocFreq(final String field) {
        return FlamdexUtils.getStringTotalDocFreq(this, field);
    }

    @Override
    public Collection<String> getAvailableMetrics() {
        return intFields;
    }

    @Override
    public final IntValueLookup getMetric(final String metric) throws FlamdexOutOfMemoryException {
        final NativeFlamdexFieldCacher fieldCacher = getMetricCacher(metric);
        try (SimpleIntTermIterator iterator = getUnsortedIntTermIterator(metric)) {
            return cacheField(iterator, metric, fieldCacher);
        }
    }

    @VisibleForTesting
    public final IntValueLookup getMetricJava(final String metric) throws FlamdexOutOfMemoryException {
        return super.getMetric(metric);
    }

    private IntValueLookup cacheField(final SimpleIntTermIterator iterator,
                                      final String metric,
                                      final NativeFlamdexFieldCacher fieldCacher) {
        final MinMax minMax = metricMinMaxes.get(metric);
        try {
            return useMMapMetrics ?
                    fieldCacher.newMMapFieldCache(iterator, numDocs, metric, directory, minMax.min, minMax.max) :
                    fieldCacher.newFieldCache(iterator, numDocs, minMax.min, minMax.max);
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public final long memoryRequired(final String metric) {
        if (useMMapMetrics) {
            return 0;
        }

        final NativeFlamdexFieldCacher fieldCacher = getMetricCacher(metric);
        return fieldCacher.memoryRequired(numDocs);
    }

    private NativeFlamdexFieldCacher getMetricCacher(final String metric) {
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
    public FieldsCardinalityMetadata getFieldsMetadata() {
        return cardinalityMetadata;
    }

    @Override
    public void close() throws IOException {
        mapCache.close();
    }

    protected static void buildIntBTrees(final Path directory, final List<String> intFields) throws IOException {
        for (final String intField : intFields) {
            final Path btreeDir = directory.resolve("fld-" + intField + ".intindex");
            final Path btreeDir64 = directory.resolve("fld-" + intField + ".intindex64");
            if (Files.notExists(btreeDir) && Files.notExists(btreeDir64)) {
                SimpleFlamdexWriter.writeIntBTree(directory, intField, btreeDir64);
            }
        }
    }

    protected static void buildStringBTrees(final Path directory, final List<String> stringFields) throws IOException {
        for (final String stringField : stringFields) {
            final Path btreeDir = directory.resolve("fld-" + stringField + ".strindex");
            if (Files.notExists(btreeDir)) {
                SimpleFlamdexWriter.writeStringBTree(directory, stringField, btreeDir);
            }
        }
    }

    public void buildAndWriteCardinalityCache(final boolean skipIfExist) throws IOException {
        if (skipIfExist && FieldsCardinalityMetadata.hasMetadataFile(getDirectory())) {
            return;
        }

        try {
            // Check if directory is writable.
            final FieldsCardinalityMetadata md = new FieldsCardinalityMetadata.Builder().build();
            md.writeToDirectory(getDirectory());
        } catch (final Exception ex) {
            return;
        }

        final FieldsCardinalityMetadata.Builder builder = new FieldsCardinalityMetadata.Builder();

        for (final String intField : intFields) {
            final FieldsCardinalityMetadata.FieldInfo info = FlamdexUtils.cacheIntFieldCardinality(intField, this);
            builder.addIntField(intField, info);
        }

        for (final String stringField : stringFields) {
            final FieldsCardinalityMetadata.FieldInfo info = FlamdexUtils.cacheStringFieldCardinality(stringField, this);
            builder.addStringField(stringField, info);
        }

        final FieldsCardinalityMetadata metadata = builder.build();
        try {
            metadata.writeToDirectory(getDirectory());
        } catch (final Exception ex) {
            // do nothing.
        }
        cardinalityMetadata = metadata;
    }

    public static final class Config {
        private boolean writeBTreesIfNotExisting = true;
        // Calculating cardinality is very expensive.
        // If defauld value of this is 'true' then there will huge cpu-load on shards opening.
        // TODO: change default to true when cardinality metadata is integrated
        // in shards building and metadata have been created for old shards.
        private boolean writeCardinalityIfNotExisting = false;
        private boolean useMMapMetrics = System.getProperty("flamdex.mmap.fieldcache") != null;
        private boolean useMMapDocIdStream = false;

        public boolean isWriteBTreesIfNotExisting() {
            return writeBTreesIfNotExisting;
        }

        public boolean isWriteCardinalityIfNotExisting() {
            return writeCardinalityIfNotExisting;
        }

        public Config setWriteBTreesIfNotExisting(final boolean writeBTreesIfNotExisting) {
            this.writeBTreesIfNotExisting = writeBTreesIfNotExisting;
            return this;
        }

        public Config setWriteCardinalityIfNotExisting( final boolean writeFieldCapacityIfNotExisting) {
            this.writeCardinalityIfNotExisting = writeFieldCapacityIfNotExisting;
            return this;
        }

        public boolean isUseMMapMetrics() {
            return useMMapMetrics;
        }

        public Config setUseMMapMetrics(final boolean useMMapMetrics) {
            this.useMMapMetrics = useMMapMetrics;
            return this;
        }

        public boolean isUseMMapDocIdStream() {
            return useMMapDocIdStream;
        }

        public void setUseMMapDocIdStream(final boolean useMMapDocIdStream) {
            this.useMMapDocIdStream = useMMapDocIdStream;
        }
    }
}

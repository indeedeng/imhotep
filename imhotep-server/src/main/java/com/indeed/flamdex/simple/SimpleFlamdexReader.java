/*
 * Copyright (C) 2018 Indeed Inc.
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
import com.google.common.collect.Sets;
import com.indeed.flamdex.AbstractFlamdexReader;
import com.indeed.flamdex.api.DocIdStream;
import com.indeed.flamdex.api.FieldsCardinalityMetadata;
import com.indeed.flamdex.api.FlamdexOutOfMemoryException;
import com.indeed.flamdex.api.GenericIntTermDocIterator;
import com.indeed.flamdex.api.GenericStringTermDocIterator;
import com.indeed.flamdex.api.IntTermDocIterator;
import com.indeed.flamdex.api.IntValueLookup;
import com.indeed.flamdex.api.StringTermDocIterator;
import com.indeed.flamdex.fieldcache.FieldCacherUtil;
import com.indeed.flamdex.fieldcache.NativeFlamdexFieldCacher;
import com.indeed.flamdex.fieldcache.UnsortedIntTermDocIterator;
import com.indeed.flamdex.reader.FlamdexMetadata;
import com.indeed.flamdex.utils.FlamdexUtils;
import com.indeed.flamdex.utils.ShardMetadataUtils;
import com.indeed.imhotep.RaceCache;

import javax.annotation.Nonnull;
import java.io.File;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;

/**
 * @author jsgroth
 */
public class SimpleFlamdexReader extends AbstractFlamdexReader {

    private final Collection<String> intFields;
    private final Collection<String> stringFields;
    private final MapCache mapCache = new MapCache();
    private FieldsCardinalityMetadata cardinalityMetadata;

    private final boolean useNativeDocIdStream;
    private final boolean useMMapDocIdStream;
    private final boolean useSSSE3;

    private final RaceCache<String, NativeFlamdexFieldCacher, RuntimeException> intFieldCachers = RaceCache.create(this::createCacher, ignored -> {});

    protected SimpleFlamdexReader(final Path directory,
                                  final int numDocs,
                                  final Collection<String> intFields,
                                  final Collection<String> stringFields,
                                  final Config config) {
        super(directory, numDocs, config.useMMapMetrics);

        this.intFields = intFields;
        this.stringFields = stringFields;

        useMMapDocIdStream = config.useMMapDocIdStream;
        useNativeDocIdStream = config.useNativeDocIdStream;
        useSSSE3 = config.useSSSE3;

        cardinalityMetadata = FieldsCardinalityMetadata.open(directory);
    }

    public static SimpleFlamdexReader open(final String directory) throws IOException {
        return open(new File(directory).toPath());
    }

    public static SimpleFlamdexReader open(final String directory, int numDocs) throws IOException {
        return open(new File(directory).toPath(), numDocs);
    }

    public static SimpleFlamdexReader open(@Nonnull final Path directory) throws IOException {
        return open(directory, new Config());
    }
    public static SimpleFlamdexReader open(@Nonnull final Path directory, int numDocs) throws IOException {
        return open(directory, new Config(), numDocs);
    }

    public static SimpleFlamdexReader open(final String directory, final Config config) throws IOException {
        return open(new File(directory).toPath(), config);
    }

    public static SimpleFlamdexReader open(final String directory, final Config config, int numDocs) throws IOException {
        return open(new File(directory).toPath(), config, numDocs);
    }

    public static SimpleFlamdexReader open(@Nonnull final Path directory, final Config config) throws IOException {
        final FlamdexMetadata metadata = FlamdexMetadata.readMetadata(directory);
        return open(directory, config, metadata.getNumDocs());
    }

    public static SimpleFlamdexReader open(@Nonnull final Path directory, final Config config, int numDocs) throws IOException {

        final List<Path> paths = new ArrayList<>();
        try (final DirectoryStream<Path> dirStream = Files.newDirectoryStream(directory)) {
            for (final Path path : dirStream) {
                paths.add(path);
            }
        }

        final ShardMetadataUtils.AllFields fields = ShardMetadataUtils.getFieldsFromFlamdexFiles(paths);
        final Collection<String> intFields = fields.intFields;
        final Collection<String> stringFields = fields.strFields;
        if (config.isWriteBTreesIfNotExisting()) {
            final Set<String> pathNames = Sets.newHashSet();
            for (Path path : paths) {
                pathNames.add(path.getFileName().toString());
            }

            buildIntBTrees(directory, pathNames, Lists.newArrayList(intFields));
            buildStringBTrees(directory, pathNames, Lists.newArrayList(stringFields));
        }
        final SimpleFlamdexReader result =
                new SimpleFlamdexReader(
                        directory,
                        numDocs,
                        intFields,
                        stringFields,
                        config);
        if (config.isWriteCardinalityIfNotExisting()) {
            // try to build and store cache
            result.buildAndWriteCardinalityCache(true);
        }
        return result;
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
            return useNativeDocIdStream ? new NativeDocIdStream(mapCache, useSSSE3) : new MMapDocIdStream(mapCache);
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
                        return new UnsortedStringToIntTermIterator.SimpleUnsortedStringToIntTermIterator(stringTermIterator);
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
                return new NativeIntTermDocIterator(termIterator, mapCache, useSSSE3);
            } else {
                return new GenericIntTermDocIterator(termIterator, getDocIdStream());
            }
        } catch (final IOException e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public StringTermDocIterator getStringTermDocIterator(final String field) {
        final SimpleStringTermIterator termIterator = getStringTermIterator(field);
        final Path termsPath = termIterator.getFilename();

        try {
            if (useNativeDocIdStream && (Files.exists(termsPath) && (Files.size(termsPath) > 0))) {
                return new NativeStringTermDocIterator(termIterator, mapCache, useSSSE3);
            } else {
                return new GenericStringTermDocIterator(termIterator, getDocIdStream());
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
        final NativeFlamdexFieldCacher fieldCacher = intFieldCachers.getOrLoad(metric);
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

        final NativeFlamdexFieldCacher fieldCacher = intFieldCachers.getOrLoad(metric);
        return fieldCacher.memoryRequired(numDocs);
    }

    private NativeFlamdexFieldCacher createCacher(String metric) {
        final MinMax minMax = new MinMax();
        final NativeFlamdexFieldCacher cacher =
                FieldCacherUtil.getNativeCacherForField(metric, this, minMax);
        metricMinMaxes.put(metric, minMax);
        return cacher;
    }

    @Override
    public FieldsCardinalityMetadata getFieldsMetadata() {
        return cardinalityMetadata;
    }

    @Override
    public void close() throws IOException {
        mapCache.close();
    }

    protected static void buildIntBTrees(final Path directory, final Set<String> dirNames, final List<String> intFields) throws IOException {
        for (final String intField : intFields) {
            final String btreeDirName = "fld-" + intField + ".intindex";
            final String btreeDir64Name = "fld-" + intField + ".intindex64";
            if (!dirNames.contains(btreeDirName) && !dirNames.contains(btreeDir64Name)) {
                final Path btreeDir64 = directory.resolve(btreeDir64Name);
                SimpleFlamdexWriter.writeIntBTree(directory, intField, btreeDir64);
            }
        }
    }

    protected static void buildStringBTrees(final Path directory, final Set<String> dirNames, final List<String> stringFields) throws IOException {
        for (final String stringField : stringFields) {
            final String btreeDirName = "fld-" + stringField + ".strindex";
            if (!dirNames.contains(btreeDirName)) {
                final Path btreeDir = directory.resolve(btreeDirName);
                SimpleFlamdexWriter.writeStringBTree(directory, stringField, btreeDir);
            }
        }
    }

    public void buildAndWriteCardinalityCache(final boolean skipIfExist) throws IOException {
        if (getDirectory().getFileSystem().isReadOnly()) {
            // TODO: throw IOException?
            return;
        }

        if (skipIfExist && FieldsCardinalityMetadata.hasMetadataFile(getDirectory())) {
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
        cardinalityMetadata = metadata;
        metadata.writeToDirectory(getDirectory());
    }

    public static final class Config {
        // TODO: should disable as ImhotepFS is read only
        private boolean writeBTreesIfNotExisting = true;
        // Calculating cardinality is very expensive.
        // If defauld value of this is 'true' then there will huge cpu-load on shards opening.
        // TODO: change default to true when cardinality metadata is integrated
        // in shards building and metadata have been created for old shards.
        private boolean writeCardinalityIfNotExisting = false;
        // true -> use mmap in field cacher, false -> use java array in field cacher
        private boolean useMMapMetrics = System.getProperty("flamdex.mmap.fieldcache") != null;

        // what class to return in getDocIdStream().
        // true -> NativeDocIdStream or MMapDocIdStream (depends on useNativeDocIdStream)
        // false -> ByteChannelDocIdStream
        private boolean useMMapDocIdStream = false;

        // 1. what class to return in getDocIdStream() when useMMapDocIdStream is true
        // true -> NativeDocIdStream
        // false -> MMapDocIdStream
        // 2. what class to return in get*TermDocIterator()
        // true -> Native*TermDocIteraror
        // false -> Generic*TermDocIterator
        private boolean useNativeDocIdStream = "true".equalsIgnoreCase(System.getProperties().getProperty("com.indeed.flamdex.simple.useNative"));

        // use SSSE3 native code in Native* classes
        // value of this variable is ignored when useNativeDocIdStream is false
        private boolean useSSSE3 = "true".equalsIgnoreCase(System.getProperty("com.indeed.flamdex.simple.useSSSE3"));

        public boolean isWriteBTreesIfNotExisting() {
            return writeBTreesIfNotExisting;
        }

        public Config setWriteBTreesIfNotExisting(final boolean writeBTreesIfNotExisting) {
            this.writeBTreesIfNotExisting = writeBTreesIfNotExisting;
            return this;
        }

        public boolean isWriteCardinalityIfNotExisting() {
            return writeCardinalityIfNotExisting;
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

        public Config setUseMMapDocIdStream(final boolean useMMapDocIdStream) {
            this.useMMapDocIdStream = useMMapDocIdStream;
            return this;
        }

        public boolean isUseNativeDocIdStream() {
            return useNativeDocIdStream;
        }

        public Config setUseNativeDocIdStream(final boolean useNativeDocIdStream) {
            this.useNativeDocIdStream = useNativeDocIdStream;
            return this;
        }

        public boolean isUseSSSE3() {
            return useSSSE3;
        }

        public Config setUseSSSE3(final boolean useSSSE3) {
            this.useSSSE3 = useSSSE3;
            return this;
        }
    }
}

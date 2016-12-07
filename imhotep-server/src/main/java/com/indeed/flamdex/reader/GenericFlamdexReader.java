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
 package com.indeed.flamdex.reader;

import com.google.common.base.Throwables;
import com.indeed.flamdex.fieldcache.FieldCacherUtil;
import com.indeed.flamdex.api.DocIdStream;
import com.indeed.flamdex.api.FlamdexOutOfMemoryException;
import com.indeed.flamdex.api.FlamdexReader;
import com.indeed.flamdex.api.GenericFlamdexFactory;
import com.indeed.flamdex.api.GenericIntTermDocIterator;
import com.indeed.flamdex.api.GenericStringTermDocIterator;
import com.indeed.flamdex.api.IntTermDocIterator;
import com.indeed.flamdex.api.IntTermIterator;
import com.indeed.flamdex.api.IntValueLookup;
import com.indeed.flamdex.api.StringTermDocIterator;
import com.indeed.flamdex.api.StringTermIterator;
import com.indeed.flamdex.api.StringValueLookup;
import com.indeed.flamdex.fieldcache.IntArrayIntValueLookup;
import com.indeed.flamdex.lucene.LuceneFlamdexReader;
import com.indeed.flamdex.ramses.RamsesFlamdexWrapper;
import com.indeed.flamdex.simple.SimpleFlamdexReader;
import com.indeed.flamdex.utils.FlamdexUtils;

import org.apache.log4j.Logger;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.ParallelReader;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Path;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collection;

/**
 * @author jplaisance
 */
public final class GenericFlamdexReader implements FlamdexReader {
    private static final Logger log = Logger.getLogger(GenericFlamdexReader.class);

    private final Path directory;

    private final GenericFlamdexFactory factory;

    private final int numDocs;
    private final Collection<String> intFields;
    private final Collection<String> stringFields;

    private GenericFlamdexReader(
            Path directory,
            GenericFlamdexFactory factory,
            int numDocs,
            Collection<String> intFields,
            Collection<String> stringFields
    ) {
        this.directory = directory;
        this.factory = factory;
        this.numDocs = numDocs;
        this.intFields = intFields;
        this.stringFields = stringFields;
    }

    /**
     * use {@link #open(Path)} instead
     */
    @Deprecated
    public static FlamdexReader open(String directory) throws IOException {
        return open(Paths.get(directory));
    }

    public static FlamdexReader open (Path directory) throws IOException {
            final FlamdexReader r = internalOpen(directory);
        if (RamsesFlamdexWrapper.ramsesFilesExist(directory)) {
            return new RamsesFlamdexWrapper(r, directory);
        }
        return r;
    }

    private static FlamdexReader internalOpen(Path directory) throws IOException {
        final Path metadataPath = directory.resolve("metadata.txt");

        if (Files.notExists(directory)) {
            throw new FileNotFoundException(directory + " does not exist");
        }

        if (!Files.isDirectory(directory)) {
            throw new FileNotFoundException(directory + " is not a directory");
        }

        if (Files.notExists(metadataPath)) {
            return new LuceneFlamdexReader(directory);
        }

        final FlamdexMetadata metadata = FlamdexMetadata.readMetadata(directory);
        switch (metadata.getFormatVersion()) {
            case 0 :
                return SimpleFlamdexReader.open(directory);
            case 1 :
                throw new UnsupportedOperationException("pfordelta is no longer supported");
            case 2 : 
                return new LuceneFlamdexReader(directory,
                                               metadata.getIntFields(),
                                               metadata.getStringFields());
            default:
                throw new IllegalArgumentException(
                        "index format version " + metadata.getFormatVersion() + " not supported");
        }
    }


    /**
     * use {@link #open(Path, GenericFlamdexFactory)} instead
     */
    @Deprecated
    public static GenericFlamdexReader open(String directory, GenericFlamdexFactory factory)
            throws IOException {
        return open(Paths.get(directory), factory);
    }

    public static GenericFlamdexReader open(Path directory, GenericFlamdexFactory factory)
            throws IOException {
        final FlamdexMetadata metadata = FlamdexMetadata.readMetadata(directory);
        return new GenericFlamdexReader(directory,
                                        factory,
                                        metadata.numDocs,
                                        metadata.intFields,
                                        metadata.stringFields);
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
    public int getNumDocs() {
        return numDocs;
    }

    @Override
    public Path getDirectory() {
        return directory;
    }

    @Override
    public DocIdStream getDocIdStream() {
        return factory.createDocIdStream();
    }

    @Override
    public IntTermIterator getIntTermIterator(String field) {
        final Path termsPath = directory.resolve(factory.getIntTermsFilename(field));
        final Path docsPath = directory.resolve(factory.getIntDocsFilename(field));
        try {
            return factory.createIntTermIterator(termsPath, docsPath);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public IntTermIterator getUnsortedIntTermIterator(String field) {
        // TODO?
        return getIntTermIterator(field);
    }

    @Override
    public StringTermIterator getStringTermIterator(String field) {
        final Path termsPath = directory.resolve(factory.getStringTermsFilename(field));
        final Path docsPath = directory.resolve(factory.getStringDocsFilename(field));
        try {
            return factory.createStringTermIterator(termsPath, docsPath);
        } catch (IOException e) {
            throw new RuntimeException(e);
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
    public IntValueLookup getMetric(String metric) throws FlamdexOutOfMemoryException {
        return new IntArrayIntValueLookup(FlamdexUtils.cacheIntField(metric, this));
    }

    public StringValueLookup getStringLookup(final String field) throws FlamdexOutOfMemoryException {
        try {
            return FieldCacherUtil.newStringValueLookup(field, this);
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public long memoryRequired(String metric) {
        return 4L * getNumDocs();
    }

    @Override
    public void close() throws IOException {
    }
}

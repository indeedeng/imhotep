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
import com.indeed.util.io.Files;
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
import com.indeed.imhotep.io.caching.CachedFile;

import org.apache.log4j.Logger;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.ParallelReader;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Collection;

/**
 * @author jplaisance
 */
public final class GenericFlamdexReader implements FlamdexReader {
    private static final Logger log = Logger.getLogger(GenericFlamdexReader.class);

    private final String directory;

    private final GenericFlamdexFactory factory;

    private final int numDocs;
    private final Collection<String> intFields;
    private final Collection<String> stringFields;

    private GenericFlamdexReader(
            String directory,
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

    public static FlamdexReader open(String directory) throws IOException {
        final FlamdexReader r = internalOpen(directory);
        if (RamsesFlamdexWrapper.ramsesFilesExist(directory)) {
            return new RamsesFlamdexWrapper(r, directory);
        }
        return r;
    }

    private static FlamdexReader internalOpen(String directory) throws IOException {
        final CachedFile dir = CachedFile.create(directory);
        final String metadataPath = CachedFile.buildPath(directory, "metadata.txt");
        final CachedFile metadataFile = CachedFile.create(metadataPath);
        
        if (! dir.exists()) {
            throw new FileNotFoundException(directory + " does not exist");
        }

        if (! dir.isDirectory()) {
            throw new FileNotFoundException(directory + " is not a directory");
        }

        if (! metadataFile.exists()) {
            final IndexReader luceneIndex;
            final File indexDir = dir.loadDirectory();
            
            if (IndexReader.indexExists(indexDir)) {
                luceneIndex = IndexReader.open(indexDir);
            } else {
                throw new IOException("directory " + directory + " does not have a metadata.txt and is not a lucene index");
            }

            // try finding and loading subindexes
            final ParallelReader pReader = new ParallelReader();
            pReader.add(luceneIndex);
            final int maxDoc = luceneIndex.maxDoc();
            final File[] files = indexDir.listFiles();
            if (files != null) {
                for (final File file : files) {
                    if (!file.isDirectory() || !IndexReader.indexExists(file)) {
                        continue; // only interested in Lucene indexes in subdirectories
                    }

                    try {
                        final IndexReader subIndexReader = IndexReader.open(file);
                        final int siMaxDoc = subIndexReader.maxDoc();
                        if (siMaxDoc != maxDoc) {
                            log.warn("unable to load subindex. (maxDoc) do not match index (" + siMaxDoc + ") != (" + maxDoc + ") for " + file.getAbsolutePath());
                            continue;
                        }
                        pReader.add(subIndexReader, true);
                    } catch (IOException e) {
                        log.warn("unable to open subindex: " + file.getAbsolutePath());
                    }
                }
            }

            return new LuceneFlamdexReader(pReader, directory);
        }

        final FlamdexMetadata metadata = FlamdexMetadata.readMetadata(directory);
        switch (metadata.getFormatVersion()) {
            case 0 : return SimpleFlamdexReader.open(directory);
            case 1 : throw new UnsupportedOperationException("pfordelta is no longer supported");
            case 2 : 
                final File indexDir = dir.loadDirectory();
                return new LuceneFlamdexReader(IndexReader.open(indexDir), 
                                               metadata.getIntFields(), 
                                               metadata.getStringFields());
        }
        throw new IllegalArgumentException("index format version "+metadata.getFormatVersion()+" not supported");
    }


    public static GenericFlamdexReader open(
            String directory,
            GenericFlamdexFactory factory
            ) throws IOException {
        final FlamdexMetadata metadata = FlamdexMetadata.readMetadata(directory);
        return new GenericFlamdexReader(directory, factory, metadata.numDocs, metadata.intFields, metadata.stringFields);
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
    public String getDirectory() {
        return directory;
    }

    @Override
    public DocIdStream getDocIdStream() {
        return factory.createDocIdStream();
    }

    @Override
    public IntTermIterator getIntTermIterator(String field) {
        final String termsFilename = Files.buildPath(directory, factory.getIntTermsFilename(field));
        final String docsFilename = Files.buildPath(directory, factory.getIntDocsFilename(field));
        try {
            return factory.createIntTermIterator(termsFilename, docsFilename);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public StringTermIterator getStringTermIterator(String field) {
        final String termsFilename = Files.buildPath(directory, factory.getStringTermsFilename(field));
        final String docsFilename = Files.buildPath(directory, factory.getStringDocsFilename(field));
        try {
            return factory.createStringTermIterator(termsFilename, docsFilename);
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
            return FieldCacherUtil.newStringValueLookup(field, this, directory);
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

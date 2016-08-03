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
 package com.indeed.flamdex.lucene;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import com.indeed.flamdex.AbstractFlamdexReader;
import com.indeed.flamdex.api.DocIdStream;
import com.indeed.flamdex.api.IntTermIterator;
import com.indeed.flamdex.api.StringTermIterator;
import com.indeed.flamdex.fieldcache.UnsortedIntTermDocIterator;
import com.indeed.flamdex.utils.FlamdexUtils;
import org.apache.log4j.Logger;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.ParallelReader;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

public class LuceneFlamdexReader extends AbstractFlamdexReader {
    private static final Logger log = Logger.getLogger(LuceneFlamdexReader.class);

    protected final IndexReader reader;

    protected final Collection<String> intFields;
    protected final Collection<String> stringFields;
    private final ListMultimap<Path, InputStream> trackingObjects = ArrayListMultimap.create();

    public LuceneFlamdexReader(final Path directory) throws IOException {
        this(directory, null, null);
    }

    public LuceneFlamdexReader(final Path directory,
                               @Nullable  final Collection<String> intFields,
                               @Nullable  final Collection<String> stringFields) throws IOException {
        super(directory);

        // verify the directory is valid
        final File indexDir = directory.toFile();
        trackDirectory(directory);
        if (!IndexReader.indexExists(indexDir)) {
            throw new IOException("directory " + directory + " is not a lucene index");
        }

        reader = buildLuceneIndexReader(directory, indexDir);
        this.intFields = (intFields != null) ? intFields : Collections.<String>emptyList();
        this.stringFields = (stringFields != null)
                ? stringFields
                : getStringFieldsFromIndex(reader);
    }

    private IndexReader buildLuceneIndexReader(Path directory, File indexDir) throws IOException {
        final ParallelReader parallelReader = new ParallelReader();

        final IndexReader topIndex = IndexReader.open(indexDir);
        final int maxDoc = topIndex.maxDoc();

        // try finding and loading subindexes
        boolean foundSubIndexes = false;
        try (final DirectoryStream<Path> entries = Files.newDirectoryStream(directory)) {
            for (Path path : entries) {
                if (!IndexReader.indexExists(path.toFile())) {
                    continue; // only interested in Lucene indexes in subdirectories
                }

                trackDirectory(path);

                try {
                    final boolean indexValid = addSubIndex(parallelReader, maxDoc, path);
                    if (indexValid)
                        foundSubIndexes = true;
                    else
                        untrackDirectory(path);
                } catch (final IOException e) {
                    untrackDirectory(path);
                    log.warn("unable to open subindex: " + path.toString());
                }
            }
        }

        setNumDocs(maxDoc);

        if (foundSubIndexes) {
            parallelReader.add(topIndex);
            return parallelReader;
        } else {
            parallelReader.close();
            return topIndex;
        }
    }

    private boolean addSubIndex(ParallelReader parallelReader, int maxDoc, Path directory) throws
            IOException {
        final IndexReader subIndexReader = IndexReader.open(directory.toFile());
        final int siMaxDoc = subIndexReader.maxDoc();
        if (siMaxDoc != maxDoc) {
            log.warn("unable to load subindex. "
                             + "(maxDoc) do not match index (" + siMaxDoc + ") "
                             + "!= (" + maxDoc + ") "
                             + "for " + directory.toString());
            subIndexReader.close();
            return false;
        }
        parallelReader.add(subIndexReader, true);
        return true;
    }

    /*
     * Necessary for caching, since lucene uses the File interface (java.io)
     * instead of the Path (java.nio.file) interface
     */
    private void trackDirectory(final Path dir) throws IOException {
        try (final DirectoryStream<Path> entries = Files.newDirectoryStream(dir)) {
            for (final Path path : entries) {
                if (Files.isDirectory(path)) {
                    continue; // only interested files
                }
                final InputStream is = Files.newInputStream(path, StandardOpenOption.READ);
                trackingObjects.put(dir, is);
            }
        }

    }

    private void untrackDirectory(final Path dir) throws IOException {
        final List<InputStream> streams = trackingObjects.get(dir);
        for (final InputStream is : streams) {
            is.close();
        }
        trackingObjects.removeAll(dir);
    }

    @Override
    public Collection<String> getIntFields() {
        return intFields;
    }

    @Override
    public Collection<String> getStringFields() {
        return stringFields;
    }

    private static Collection<String> getStringFieldsFromIndex(final IndexReader reader) {
        final Collection<String> ret = new HashSet<String>();
        // don't like having to use Object and downcast,
        // but in Lucene versions prior to 3 getFieldNames()
        // returns an un-genericized Collection instead of a Collection<String>
        for (final Object o : reader.getFieldNames(IndexReader.FieldOption.INDEXED)) {
            ret.add((String)o);
        }
        return ret;
    }

    @Override
    public int getNumDocs() {
        return reader.maxDoc();
    }

    @Override
    public DocIdStream getDocIdStream() {
        try {
            return new LuceneDocIdStream(reader.termDocs());
        } catch (IOException e) {
            throw LuceneUtils.ioRuntimeException(e);
        }
    }

    @Override
    public IntTermIterator getIntTermIterator(final String field) {
        return new LuceneIntTermIterator(reader, field);
    }

    @Override
    public IntTermIterator getUnsortedIntTermIterator(final String field) {
        return new LuceneUnsortedIntTermIterator(reader, field);
    }

    @Override
    public StringTermIterator getStringTermIterator(final String field) {
        return new LuceneStringTermIterator(reader, field);
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
    protected UnsortedIntTermDocIterator createUnsortedIntTermDocIterator(String field) {
        try {
            return LuceneUnsortedIntTermDocIterator.create(reader, field);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() throws IOException {
        for (InputStream is : trackingObjects.values()) {
            is.close();
        }
        reader.close();
    }
}

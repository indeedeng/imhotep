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
 package com.indeed.flamdex.writer;

import com.google.common.io.Closer;
import com.indeed.flamdex.api.DocIdStream;
import com.indeed.flamdex.api.FlamdexReader;
import com.indeed.flamdex.api.IntTermIterator;
import com.indeed.flamdex.api.StringTermIterator;
import com.indeed.flamdex.reader.FlamdexFormatVersion;
import com.indeed.flamdex.reader.FlamdexMetadata;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * @author jplaisance
 */
public final class GenericFlamdexWriter implements FlamdexWriter {

    private static final int DOC_ID_BUFFER_SIZE = 32;

    private final Path outputDirectory;

    private final IntFieldWriterFactory intFieldWriterFactory;

    private final StringFieldWriterFactory stringFieldWriterFactory;

    private long maxDocs;

    private final FlamdexFormatVersion formatVersion;

    private final Set<String> intFields;
    private final Set<String> stringFields;

    public GenericFlamdexWriter(
            final Path outputDirectory,
            final IntFieldWriterFactory intFieldWriterFactory,
            final StringFieldWriterFactory stringFieldWriterFactory,
            final long numDocs,
            final FlamdexFormatVersion formatVersion) throws IOException {
        this(outputDirectory, intFieldWriterFactory, stringFieldWriterFactory, numDocs, formatVersion, true);
    }

    public GenericFlamdexWriter(
            final Path outputDirectory,
            final IntFieldWriterFactory intFieldWriterFactory,
            final StringFieldWriterFactory stringFieldWriterFactory,
            final long numDocs,
            final FlamdexFormatVersion formatVersion,
            final boolean create) throws IOException {
        this.outputDirectory = outputDirectory;
        this.intFieldWriterFactory = intFieldWriterFactory;
        this.stringFieldWriterFactory = stringFieldWriterFactory;
        this.maxDocs = numDocs;
        this.formatVersion = formatVersion;
        if (create) {
            intFields = new HashSet<>();
            stringFields = new HashSet<>();
        } else {
            final FlamdexMetadata metadata = FlamdexMetadata.readMetadata(outputDirectory);
            if (metadata.getNumDocs() != numDocs) {
                throw new IllegalArgumentException("numDocs does not match numDocs in existing index");
            }
            intFields = new HashSet<>(metadata.getIntFields());
            stringFields = new HashSet<>(metadata.getStringFields());
        }
    }

    @Override
    public IntFieldWriter getIntFieldWriter(final String field) throws IOException {
        return getIntFieldWriter(field, false);
    }

    public IntFieldWriter getIntFieldWriter(final String field, final boolean blowAway) throws IOException {
        if (!blowAway && intFields.contains(field)) {
            throw new IllegalArgumentException("already added int field "+field);
        }
        intFields.add(field);
        return intFieldWriterFactory.create(outputDirectory, field, maxDocs);
    }

    @Override
    public StringFieldWriter getStringFieldWriter(final String field) throws IOException {
        return getStringFieldWriter(field, false);
    }

    public StringFieldWriter getStringFieldWriter(final String field, final boolean blowAway) throws IOException {
        if (!blowAway && stringFields.contains(field)) {
            throw new IllegalArgumentException("already added string field "+field);
        }
        stringFields.add(field);
        return stringFieldWriterFactory.create(outputDirectory, field, maxDocs);
    }

    @Override
    public Path getOutputDirectory() {
        return this.outputDirectory;
    }
    
    @Override
    public void resetMaxDocs(final long maxDocs) {
        this.maxDocs = maxDocs;
    }

    @Override
    public void close() throws IOException {
        final List<String> intFieldsList = new ArrayList<>(intFields);
        Collections.sort(intFieldsList);

        final List<String> stringFieldsList = new ArrayList<>(stringFields);
        Collections.sort(stringFieldsList);

        final FlamdexMetadata metadata = new FlamdexMetadata((int)maxDocs, intFieldsList, stringFieldsList, formatVersion);
        FlamdexMetadata.writeMetadata(outputDirectory, metadata);
    }

    public static void writeFlamdex(
            final Path indexDir,
            final FlamdexReader fdx,
            final IntFieldWriterFactory intFieldWriterFactory,
            final StringFieldWriterFactory stringFieldWriterFactory,
            final FlamdexFormatVersion formatVersion,
            final List<String> intFields,
            final List<String> stringFields) throws IOException {
        try (Closer closer = Closer.create()) {
            final DocIdStream dis = fdx.getDocIdStream();
            closer.register(dis);
            final int[] docIdBuf = new int[DOC_ID_BUFFER_SIZE];

            final GenericFlamdexWriter w = new GenericFlamdexWriter(indexDir, intFieldWriterFactory, stringFieldWriterFactory, fdx.getNumDocs(), formatVersion);
            closer.register(new Closeable() {
                @Override
                public void close() throws IOException {
                    w.close();
                }
            });

            for (final String intField : intFields) {
                try (Closer intIterCloser = Closer.create()) {
                    final IntFieldWriter ifw = w.getIntFieldWriter(intField);
                    intIterCloser.register(new Closeable() {
                        @Override
                        public void close() throws IOException {
                            ifw.close();
                        }
                    });
                    final IntTermIterator iter = fdx.getIntTermIterator(intField);
                    intIterCloser.register(iter);
                    while (iter.next()) {
                        ifw.nextTerm(iter.term());
                        dis.reset(iter);
                        while (true) {
                            final int n = dis.fillDocIdBuffer(docIdBuf);
                            for (int i = 0; i < n; ++i) {
                                ifw.nextDoc(docIdBuf[i]);
                            }
                            if (n < docIdBuf.length) {
                                break;
                            }
                        }
                    }
                }
            }

            for (final String stringField : stringFields) {
                try (Closer strIterCloser = Closer.create()) {
                    final StringFieldWriter sfw = w.getStringFieldWriter(stringField);
                    strIterCloser.register(new Closeable() {
                        @Override
                        public void close() throws IOException {
                            sfw.close();
                        }
                    });
                    final StringTermIterator iter = fdx.getStringTermIterator(stringField);
                    strIterCloser.register(iter);
                    while (iter.next()) {
                        sfw.nextTerm(iter.term());
                        dis.reset(iter);
                        while (true) {
                            final int n = dis.fillDocIdBuffer(docIdBuf);
                            for (int i = 0; i < n; ++i) {
                                sfw.nextDoc(docIdBuf[i]);
                            }
                            if (n < docIdBuf.length) {
                                break;
                            }
                        }
                    }
                }
            }
        }
    }
}

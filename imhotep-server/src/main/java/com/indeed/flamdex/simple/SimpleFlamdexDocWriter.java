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

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.io.Closer;
import com.indeed.flamdex.MemoryFlamdex;
import com.indeed.flamdex.api.FlamdexReader;
import com.indeed.flamdex.writer.FlamdexDocWriter;
import com.indeed.flamdex.writer.FlamdexDocument;
import com.indeed.flamdex.writer.FlamdexWriter;
import com.indeed.util.core.io.Closeables2;
import com.indeed.util.core.shell.PosixFileOperations;
import com.indeed.util.io.BufferedFileDataInputStream;
import com.indeed.util.io.BufferedFileDataOutputStream;
import org.apache.log4j.Logger;

import java.io.Closeable;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

/**
 * @author jsgroth
 */
public final class SimpleFlamdexDocWriter implements FlamdexDocWriter {
    private static final Logger LOGGER = Logger.getLogger(SimpleFlamdexDocWriter.class);
    private final Path outputDirectory;
    private final int docBufferSize;
    private final int mergeFactor;
    private final boolean addToExistingIndex;

    private final List<List<Path>> segmentsOnDisk;

    private MemoryFlamdex currentBuffer = new MemoryFlamdex();
    private String currentSegment = "_0";

    /**
     * use {@link #SimpleFlamdexDocWriter(Path, Config)} instead
     */
    @Deprecated
    public SimpleFlamdexDocWriter(final String outputDirectory, final Config config) throws IOException {
        this(Paths.get(outputDirectory), config);
    }

    public SimpleFlamdexDocWriter(final Path outputDirectory, final Config config) throws IOException {
        createOutputDir(outputDirectory);

        this.outputDirectory = outputDirectory;
        this.docBufferSize = config.getDocBufferSize();
        this.mergeFactor = config.getMergeFactor();
        this.addToExistingIndex = config.isAddToExistingIndex();

        segmentsOnDisk = Lists.newArrayList();
        segmentsOnDisk.add(new ArrayList<Path>());
    }

    private static void createOutputDir(final Path outputDirectory) throws IOException {
        if (Files.exists(outputDirectory) && !Files.isDirectory(outputDirectory)) {
            throw new FileNotFoundException(outputDirectory + " is not a directory");
        }

        try {
            Files.createDirectories(outputDirectory);
        } catch (final IOException e) {
            throw new IOException("unable to create directory " + outputDirectory, e);
        }
    }

    @Override
    public void addDocument(final FlamdexDocument doc) throws IOException {
        currentBuffer.addDocument(doc);
        if (currentBuffer.getNumDocs() == docBufferSize) {
            flush();
            currentBuffer = new MemoryFlamdex();
        }
    }

    private void flush() throws IOException {
        if (currentBuffer.getNumDocs() == 0) {
            return;
        }

        final Path outFile = outputDirectory.resolve(currentSegment);
        try (BufferedFileDataOutputStream out = new BufferedFileDataOutputStream(outFile, ByteOrder.nativeOrder(), 65536)) {
            currentBuffer.write(out);
        }

        segmentsOnDisk.get(0).add(outFile);

        currentSegment = nextSegmentDirectory(currentSegment);

        int i = 0;
        while (segmentsOnDisk.get(i).size() == mergeFactor) {
            final List<Path> segments = segmentsOnDisk.get(i);
            final List<FlamdexReader> readers = Lists.newArrayListWithCapacity(segments.size());
            final Path mergeDir;
            final Closer closer = Closer.create();
            try {
                long numDocs = 0;
                for (final Path segment : segments) {
                    final FlamdexReader reader;
                    if (i == 0) {
                        final BufferedFileDataInputStream dataInputStream = new BufferedFileDataInputStream(segment,
                                ByteOrder.nativeOrder(),
                                65536);
                        reader = MemoryFlamdex.streamer(dataInputStream);
                    } else {
                        reader = SimpleFlamdexReader.open(segment,
                                new SimpleFlamdexReader.Config().setWriteBTreesIfNotExisting(false));
                    }
                    readers.add(reader);
                    closer.register(reader);
                    numDocs += reader.getNumDocs();
                }

                mergeDir = outputDirectory.resolve(currentSegment);
                currentSegment = nextSegmentDirectory(currentSegment);
                final FlamdexWriter w = new SimpleFlamdexWriter(mergeDir, numDocs, true, false);
                SimpleFlamdexWriter.merge(readers, w);
                w.close();
            } finally {
                Closeables2.closeQuietly(closer, LOGGER);
            }

            for (final Path segment : segments) {
                PosixFileOperations.rmrf(segment);
            }
            segments.clear();

            if (i == segmentsOnDisk.size() - 1) {
                segmentsOnDisk.add(new ArrayList<Path>());
            }
            segmentsOnDisk.get(i + 1).add(mergeDir);

            ++i;
        }
    }

    @Override
    public void close() throws IOException {
        flush();
        long numDocs = 0;
        final List<FlamdexReader> allReaders = Lists.newArrayList();
        final Closer closer = Closer.create();
        try {
            for (final Path file : Iterables.concat(Lists.reverse(segmentsOnDisk.subList(1, segmentsOnDisk.size())))) {
                final SimpleFlamdexReader reader = SimpleFlamdexReader.open(file,
                        new SimpleFlamdexReader.Config().setWriteBTreesIfNotExisting(false));
                allReaders.add(reader);
                closer.register(reader);
                numDocs += reader.getNumDocs();
            }
            for (final Path path : segmentsOnDisk.get(0)) {
                final FlamdexReader reader = MemoryFlamdex.streamer(new BufferedFileDataInputStream(path, ByteOrder.nativeOrder(), 65536));
                allReaders.add(reader);
                closer.register(reader);
                numDocs += reader.getNumDocs();
            }

            final FlamdexWriter w = new SimpleFlamdexWriter(outputDirectory, numDocs, !addToExistingIndex, true);
            closer.register(new Closeable() {
                @Override
                public void close() throws IOException {
                    w.close();
                }
            });
            SimpleFlamdexWriter.merge(allReaders, w);
        } finally {
            Closeables2.closeQuietly(closer, LOGGER);
        }

        for (final Path path : Iterables.concat(segmentsOnDisk)) {
            PosixFileOperations.rmrf(path);
        }
    }

    private static String nextSegmentDirectory(final String s) {
        int i = s.length() - 1;
        while (s.charAt(i) == 'z') {
            --i;
        }

        final StringBuilder sb = new StringBuilder(i == 0 ? s.length() + 1 : s.length());
        sb.append(s.substring(0, i));
        if (i == 0) {
            sb.append("_0");
        } else {
            sb.append(s.charAt(i) == '9' ? 'a' : (char) (s.charAt(i) + 1));
        }
        for (int j = i + 1; j < s.length(); ++j) {
            sb.append('0');
        }
        return sb.toString();
    }

    public static class Config {
        private int docBufferSize = 500;
        private int mergeFactor = 100;
        private boolean addToExistingIndex = false;

        public int getDocBufferSize() {
            return docBufferSize;
        }

        public int getMergeFactor() {
            return mergeFactor;
        }

        public boolean isAddToExistingIndex() {
            return addToExistingIndex;
        }

        public Config setDocBufferSize(final int docBufferSize) {
            this.docBufferSize = docBufferSize;
            return this;
        }

        public Config setMergeFactor(final int mergeFactor) {
            this.mergeFactor = mergeFactor;
            return this;
        }

        public Config setAddToExistingIndex(final boolean addToExistingIndex) {
            this.addToExistingIndex = addToExistingIndex;
            return this;
        }
    }
}

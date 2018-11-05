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

package com.indeed.flamdex.dynamic;

import com.google.common.base.Optional;
import com.indeed.flamdex.api.DocIdStream;
import com.indeed.flamdex.api.IntTermIterator;
import com.indeed.flamdex.api.StringTermIterator;
import com.indeed.flamdex.dynamic.locks.MultiThreadFileLockUtil;
import com.indeed.flamdex.dynamic.locks.MultiThreadLock;
import com.indeed.flamdex.simple.SimpleFlamdexWriter;
import com.indeed.flamdex.writer.IntFieldWriter;
import com.indeed.flamdex.writer.StringFieldWriter;
import com.indeed.util.core.shell.PosixFileOperations;
import com.indeed.util.io.Directories;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * @author michihiko
 */

public final class DynamicFlamdexIndexUtil {
    // {dataset}/{indexDirectory}/reader.lock : Taken while there is a reader reads from the index.
    private static final String READER_LOCK_FILENAME = "reader.lock";
    // {dataset}/{indexDirectoryPrefix}.writer.lock : Taken while there is a writer writes into the index.
    private static final String WRITER_LOCK_FILENAME_FORMAT = "%s.writer.lock";

    private DynamicFlamdexIndexUtil() {
    }

    @Nonnull
    public static MultiThreadLock acquireReaderLock(@Nonnull final Path indexDirectory) throws IOException {
        final Optional<MultiThreadLock> readLockOrAbsent = MultiThreadFileLockUtil.tryReadLock(indexDirectory, READER_LOCK_FILENAME);
        if (!readLockOrAbsent.isPresent()) {
            throw new IOException("Failed to open index " + indexDirectory);
        }
        return readLockOrAbsent.get();
    }

    @Nonnull
    static MultiThreadLock acquireWriterLock(@Nonnull final Path datasetDirectory, @Nonnull final String indexDirectoryPrefix) throws IOException {
        return MultiThreadFileLockUtil.writeLock(datasetDirectory, String.format(WRITER_LOCK_FILENAME_FORMAT, indexDirectoryPrefix));
    }

    @Nonnull
    static List<Path> listSegmentDirectories(@Nonnull final Path indexDirectory) throws IOException {
        final List<Path> segmentDirectories = new ArrayList<>();
        for (final Path segmentDirectory : Directories.list(indexDirectory)) {
            if (Files.isDirectory(segmentDirectory)) {
                segmentDirectories.add(segmentDirectory);
            }
        }
        Collections.sort(segmentDirectories);
        return segmentDirectories;
    }

    static void removeDirectoryRecursively(@Nonnull final Path path) throws IOException {
        PosixFileOperations.rmrf(path);
    }

    static void createHardLinksRecursively(@Nonnull final Path source, @Nonnull final Path dest) throws IOException {
        PosixFileOperations.cplr(source, dest);
    }

    public static boolean tryRemoveIndex(@Nonnull final Path indexDirectory) throws IOException {
        final Optional<MultiThreadLock> lockOrEmpty = MultiThreadFileLockUtil.tryWriteLock(indexDirectory, READER_LOCK_FILENAME);
        if (lockOrEmpty.isPresent()) {
            try {
                final Path tempDirectoryName = indexDirectory.getParent().resolve("deleting." + indexDirectory.getFileName().toString());
                while (true) {
                    try {
                        Files.move(indexDirectory, tempDirectoryName);
                        removeDirectoryRecursively(tempDirectoryName);
                        return true;
                    } catch (final FileAlreadyExistsException e) {
                        // In the case we already have that temp directory, remove that first and retry.
                        removeDirectoryRecursively(tempDirectoryName);
                    }
                }
            } finally {
                lockOrEmpty.get().close();
            }
        } else {
            return false;
        }
    }

    public static class DocIdMapping {
        private final int newNumDocs;
        private final int[] newDocIds;
        private final int[] offset;

        private DocIdMapping(final int newNumDocs, @Nonnull final int[] newDocIds, @Nonnull final int[] offset) {
            this.newNumDocs = newNumDocs;
            this.newDocIds = newDocIds;
            this.offset = offset;
        }

        /**
         * @param segmentId      Index of the ald segment.
         * @param docIdInSegment Segment-local document ID of the document.
         * @return Segment-local ID of the document in the new segment, or -1 if it has been deleted.
         */
        int getNewDocId(final int segmentId, final int docIdInSegment) {
            return newDocIds[docIdInSegment + offset[segmentId]];
        }

        int getNewNumDocs() {
            return newNumDocs;
        }
    }

    @Nonnull
    static DocIdMapping mergeSegments(@Nonnull final Path outputPath, @Nonnull final List<SegmentReader> segmentReaders) throws IOException {
        final int numSegments = segmentReaders.size();
        Path dir = segmentReaders.get(0).getDirectory();
        if (dir != null) {
            dir = dir.getParent();
        }
        try (final DynamicFlamdexReader reader = new DynamicFlamdexReader(dir, segmentReaders)) {
            final int numDocs = reader.getNumDocs();
            final int[] newDocIds = new int[numDocs];

            final int[] offset = new int[numSegments + 1];
            for (int segmentIdx = 0; segmentIdx < segmentReaders.size(); segmentIdx++) {
                offset[segmentIdx + 1] = offset[segmentIdx] + segmentReaders.get(segmentIdx).maxNumDocs();
            }
            int newNumDoc = 0;
            for (int segmentId = 0; segmentId < segmentReaders.size(); ++segmentId) {
                final SegmentReader segmentReader = segmentReaders.get(segmentId);
                for (int docId = 0; docId < segmentReaders.get(segmentId).maxNumDocs(); ++docId) {
                    if (segmentReader.isDeleted(docId)) {
                        newDocIds[docId + offset[segmentId]] = -1;
                    } else {
                        newDocIds[docId + offset[segmentId]] = newNumDoc;
                        newNumDoc++;
                    }
                }
            }
            try (final SimpleFlamdexWriter writer = new SimpleFlamdexWriter(outputPath, newNumDoc)) {
                final int[] docIdBuf = new int[256];
                try (final DocIdStream docIdStream = reader.getDocIdStream()) {
                    for (final String field : reader.getIntFields()) {
                        try (
                                final IntFieldWriter intFieldWriter = writer.getIntFieldWriter(field);
                                final IntTermIterator intTermIterator = reader.getIntTermIterator(field)
                        ) {
                            while (intTermIterator.next()) {
                                intFieldWriter.nextTerm(intTermIterator.term());
                                docIdStream.reset(intTermIterator);
                                while (true) {
                                    final int num = docIdStream.fillDocIdBuffer(docIdBuf);
                                    for (int i = 0; i < num; ++i) {
                                        final int docId = newDocIds[docIdBuf[i]];
                                        if (docId >= 0) {
                                            intFieldWriter.nextDoc(docId);
                                        }
                                    }
                                    if (num < docIdBuf.length) {
                                        break;
                                    }
                                }
                            }
                        }
                    }
                    for (final String field : reader.getStringFields()) {
                        try (
                                final StringFieldWriter stringFieldWriter = writer.getStringFieldWriter(field);
                                final StringTermIterator stringTermIterator = reader.getStringTermIterator(field)
                        ) {
                            while (stringTermIterator.next()) {
                                stringFieldWriter.nextTerm(stringTermIterator.term());
                                docIdStream.reset(stringTermIterator);
                                while (true) {
                                    final int num = docIdStream.fillDocIdBuffer(docIdBuf);
                                    for (int i = 0; i < num; ++i) {
                                        final int docId = newDocIds[docIdBuf[i]];
                                        if (docId >= 0) {
                                            stringFieldWriter.nextDoc(docId);
                                        }
                                    }
                                    if (num < docIdBuf.length) {
                                        break;
                                    }
                                }
                            }
                        }
                    }
                }
                return new DocIdMapping(newNumDoc, newDocIds, offset);
            }
        }
    }

    @Nonnull
    public static DocIdMapping mergeSegments(@Nonnull final Path outputPath, @Nonnull final DynamicFlamdexReader dynamicFlamdexReader) throws IOException {
        return mergeSegments(outputPath, dynamicFlamdexReader.getSegmentReaders());
    }
}

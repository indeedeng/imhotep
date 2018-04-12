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
 package com.indeed.imhotep.archive;

import com.google.common.io.ByteStreams;
import com.indeed.imhotep.archive.compression.SquallArchiveCompressor;
import com.indeed.util.io.Files;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import static com.indeed.imhotep.archive.compression.SquallArchiveCompressor.GZIP;
import static com.indeed.imhotep.archive.compression.SquallArchiveCompressor.NONE;
import static com.indeed.imhotep.archive.compression.SquallArchiveCompressor.SNAPPY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author jsgroth
 */
public class TestSquallArchive {
    @Test
    public void test() throws IOException {
        final FileSystem fs = new NicerLocalFileSystem();
        final Path tempDir = new Path(getTempDir());
        fs.mkdirs(tempDir);
        try {
            for (final SquallArchiveCompressor compressor : Arrays.asList(NONE, GZIP, SNAPPY)) {
                final String localTempDir = getTempDir();
                try {
                    doTheTest(fs, tempDir, new File(localTempDir), compressor);
                } finally {
                    Files.delete(localTempDir);
                }
            }
        } finally {
            fs.delete(tempDir, true);
        }

        fs.mkdirs(tempDir);
        try {
            final String localTempDir = getTempDir();
            try {
                testDirectories(fs, tempDir, new File(localTempDir));
            } finally {
                Files.delete(localTempDir);
            }
        } finally {
            fs.delete(tempDir, true);
        }
    }

    private static String getTempDir() {
        return com.google.common.io.Files.createTempDir().getAbsolutePath();
    }

    private static void testDirectories(final FileSystem fs, final Path tempDir, final File localTempDir) throws IOException {
        final File localArchiveDir = new File(localTempDir, "tmp");
        if (!localArchiveDir.mkdir()) {
            throw new IOException();
        }

        try( final OutputStream os = new FileOutputStream(new File(localArchiveDir, "tempfile")) ) {
            for (int i = 1; i <= 10; ++i) {
                os.write(i);
            }
        }

        final SquallArchiveWriter writer = new SquallArchiveWriter(fs, tempDir, true);
        writer.appendDirectory(localArchiveDir);
        writer.commit();

        checkDirectory(fs, tempDir, localTempDir, localArchiveDir);

        writer.commit(); // no-op
        checkDirectory(fs, tempDir, localTempDir, localArchiveDir);

        final SquallArchiveWriter writer2 = new SquallArchiveWriter(fs, tempDir, true);
        writer2.batchAppendDirectory(localTempDir);

        checkDirectory(fs, tempDir, localTempDir, localArchiveDir);        
    }

    private static void checkDirectory(final FileSystem fs, final Path tempDir, final File localTempDir, final File localArchiveDir) throws IOException {
        Files.delete(localArchiveDir.getAbsolutePath());
        final SquallArchiveReader reader = new SquallArchiveReader(fs, tempDir);
        reader.copyToLocal("tmp/tempfile", localTempDir.getAbsolutePath());
        assertTrue(ByteStreams.equal(
                com.google.common.io.Files.newInputStreamSupplier(new File(localArchiveDir, "tempfile")),
                ByteStreams.newInputStreamSupplier(new byte[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10})
        ));

        Files.delete(localArchiveDir.getAbsolutePath());
        reader.copyAllToLocal(localTempDir.getAbsolutePath());
        assertTrue(ByteStreams.equal(
                com.google.common.io.Files.newInputStreamSupplier(new File(localArchiveDir, "tempfile")),
                ByteStreams.newInputStreamSupplier(new byte[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10})
        ));
    }

    private static void doTheTest(final FileSystem fs, final Path tempDir, final File localTempDir, final SquallArchiveCompressor compressor) throws IOException {
        final Random rand = new Random();
        final List<File> tempFiles = new ArrayList<>();
        for (int i = 0; i < 10; ++i) {
            final File tempFile = new File(localTempDir, "tempfile" + i);
            tempFiles.add(tempFile);
            final int len = rand.nextInt(1024) + 1024;
            try( final OutputStream os = new FileOutputStream(tempFile) ) {
                for (int j = 0; j < len; ++j) {
                    os.write(rand.nextInt(256));
                }
            }
        }

        Collections.shuffle(tempFiles);

        long bytesWritten = writeHalfTheFiles(fs, tempDir, tempFiles, compressor);
        if (compressor == NONE) {
            assertEquals(bytesWritten, getArchiveBytesWritten(fs, tempDir));
        }

        final File localTempDir2 = new File(getTempDir());
        try {
            readHalfTheFiles(fs, tempDir, localTempDir2, tempFiles);
        } finally {
            Files.delete(localTempDir2.getAbsolutePath());
        }

        bytesWritten += writeTheOtherHalf(fs, tempDir, tempFiles, compressor);
        if (compressor == NONE) {
            assertEquals(bytesWritten, getArchiveBytesWritten(fs, tempDir));
        }

        if (!localTempDir2.mkdirs()) {
            throw new IOException();
        }

        try {
            readAllTheFiles(fs, tempDir, localTempDir2, tempFiles);
        } finally {
            Files.delete(localTempDir2.getAbsolutePath());
        }
    }

    private static long getArchiveBytesWritten(final FileSystem fs, final Path tempDir) throws IOException {
        long sum = 0;
        for (final FileStatus status : fs.listStatus(tempDir, new PathFilter() {
            @Override
            public boolean accept(final Path path) {
                return SquallArchiveWriter.ARCHIVE_FILENAME_PATTERN.matcher(path.getName()).matches();
            }
        })) {
            sum += status.getLen();
        }
        return sum;
    }

    private static void readAllTheFiles(final FileSystem fs, final Path tempDir, final File localTempDir2, final List<File> tempFiles) throws IOException {
        final SquallArchiveReader reader = new SquallArchiveReader(fs, tempDir);
        final List<FileMetadata> metadata = reader.readMetadata();
        assertEquals(metadata.size(), tempFiles.size());
        for (int i = 0; i < metadata.size(); ++i) {
            final FileMetadata file = metadata.get(i);
            final File tf = tempFiles.get(i);
            assertEquals(tf.getName(), file.getFilename());
            reader.copyToLocal(file, localTempDir2);
            assertTrue(com.google.common.io.Files.equal(tf, new File(localTempDir2, file.getFilename())));
        }

        for (final File f : localTempDir2.listFiles()) {
            if (!f.delete()) {
                throw new IOException();
            }
        }

        reader.copyToLocal("tempfile3", localTempDir2.getAbsolutePath());
        assertTrue(com.google.common.io.Files.equal(new File(localTempDir2, "tempfile3"), findTempFile3(tempFiles)));
    }

    private static File findTempFile3(final List<File> tempFiles) {
        for (final File f : tempFiles) {
            if (f.getName().endsWith("3")) {
                return f;
            }
        }
        throw new AssertionError("wtf");
    }

    private static void readHalfTheFiles(final FileSystem fs, final Path tempDir, final File localTempDir2, final List<File> tempFiles) throws IOException {
        final SquallArchiveReader reader = new SquallArchiveReader(fs, tempDir);
        final List<FileMetadata> metadata = reader.readMetadata();
        assertEquals(metadata.size(), tempFiles.size() / 2);
        for (int i = 0; i < metadata.size(); ++i) {
            final FileMetadata file = metadata.get(i);
            final File tf = tempFiles.get(i);
            assertEquals(tf.getName(), file.getFilename());
            reader.copyToLocal(file, localTempDir2);
            assertTrue(ByteStreams.equal(
                    com.google.common.io.Files.newInputStreamSupplier(tf),
                    com.google.common.io.Files.newInputStreamSupplier(new File(localTempDir2, file.getFilename()))
            ));
        }
    }

    private static long writeTheOtherHalf(final FileSystem fs, final Path tempDir, final List<File> tempFiles, final SquallArchiveCompressor compressor) throws IOException {
        final SquallArchiveWriter writer = new SquallArchiveWriter(fs, tempDir, false, compressor);
        long expectedLen = 0L;
        for (int i = tempFiles.size() / 2; i < tempFiles.size(); ++i) {
            final File file = tempFiles.get(i);
            writer.appendFile(file);
            expectedLen += file.length();
        }
        writer.commit();
        return expectedLen;
    }

    private static long writeHalfTheFiles(final FileSystem fs, final Path tempDir, final List<File> tempFiles, final SquallArchiveCompressor compressor) throws IOException {
        final SquallArchiveWriter writer = new SquallArchiveWriter(fs, tempDir, true, compressor);
        long expectedLen = 0L;
        for (int i = 0; i < tempFiles.size() / 2; ++i) {
            final File file = tempFiles.get(i);
            writer.appendFile(file);
            expectedLen += file.length();
        }
        writer.commit();
        return expectedLen;
    }
}

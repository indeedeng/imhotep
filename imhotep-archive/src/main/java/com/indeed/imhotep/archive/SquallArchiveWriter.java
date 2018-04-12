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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.io.ByteStreams;
import com.indeed.imhotep.archive.compression.SquallArchiveCompressor;
import com.indeed.util.compress.CompressionOutputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.security.DigestOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author jsgroth
 */
public class SquallArchiveWriter {
    private static final Joiner TAB = Joiner.on("\t");
    private static final Joiner SLASH = Joiner.on("/");

    @VisibleForTesting
    static final Pattern ARCHIVE_FILENAME_PATTERN = Pattern.compile("^archive(\\d+)\\.bin$");

    private final FileSystem fs;
    private final Path path;

    private final List<FileMetadata> pendingMetadataWrites;

    private final SquallArchiveCompressor defaultCompressor;

    private int archivePathCounter;

    /**
     * create an archive writer
     * be aware that if create is set to true, any existing archive at the given path will be destroyed
     * if there is currently no archive at the given path, create MUST be set to true or the first call to appendFile
     *   will throw FileNotFoundException
     *
     * @param fs a file system implementation
     * @param path the directory to write this archive to
     * @param create whether to create from scratch or append to
     * @throws IOException if there is an IO problem
     */
    public SquallArchiveWriter(final FileSystem fs, final Path path, final boolean create) throws IOException {
        this(fs, path, create, SquallArchiveCompressor.NONE);
    }

    public SquallArchiveWriter(final FileSystem fs, final Path path, final boolean create, final SquallArchiveCompressor defaultCompressor) throws IOException {
        this.fs = fs;
        this.path = path;

        pendingMetadataWrites = Lists.newArrayList();

        this.defaultCompressor = defaultCompressor;

        if (create) {
            archivePathCounter = 0;
            fs.create(new Path(path, "metadata.txt"), true).close();
            deleteExistingArchiveFiles(fs, path);
        } else {
            archivePathCounter = computeCurrentArchivePathCounter(fs, path);
        }
    }

    private static void deleteExistingArchiveFiles(final FileSystem fs, final Path path) throws IOException {
        for (final FileStatus status : fs.listStatus(path, new PathFilter() {
            @Override
            public boolean accept(final Path path) {
                return ARCHIVE_FILENAME_PATTERN.matcher(path.getName()).matches();
            }
        })) {
            fs.delete(status.getPath(), true);
        }
    }

    private static int computeCurrentArchivePathCounter(final FileSystem fs, final Path path) throws IOException {
        int max = -1;
        for (final FileStatus status : fs.listStatus(path)) {
            final String pathName = status.getPath().getName();
            final Matcher matcher = ARCHIVE_FILENAME_PATTERN.matcher(pathName);
            if (matcher.matches()) {
                final String numberString = matcher.group(1);
                final int number = Integer.parseInt(numberString);
                max = Math.max(max, number);
            }
        }
        return max + 1;
    }

    private Path newArchivePath() {
        return new Path(path, "archive" + (archivePathCounter++) + ".bin");
    }

    /**
     * convenience method, calls appendFile or appendDirectory depending on whether or not file refers to a directory
     * does not modify metadata.txt until {@link #commit()} is called
     *
     * @param file the file or directory to append
     * @throws IOException if there is an IO problem
     */
    public void append(final File file) throws IOException {
        append(file, defaultCompressor);
    }

    public void append(final File file, final SquallArchiveCompressor compressor) throws IOException {
        if (file.isDirectory()) {
            appendDirectory(file, compressor);
        } else {
            appendFile(file, compressor);
        }
    }

    /**
     * recursively appends a directory, stripping the root directory name from the filenames
     * this is safer than appendDirectory because it only makes one call to {@link org.apache.hadoop.fs.FileSystem#append(Path)}
     * there is no need to call {@link #commit()} after calling this method
     *
     * @param directory the directory to append
     * @throws IOException if there is an IO problem
     */
    public void batchAppendDirectory(final File directory) throws IOException {
        batchAppendDirectory(directory, defaultCompressor);
    }

    public void batchAppendDirectory(final File directory, final SquallArchiveCompressor compressor) throws IOException {
        if (!directory.isDirectory()) {
            throw new FileNotFoundException(directory.getAbsolutePath() + " is not a directory");
        }

        batchAppend(Arrays.asList(sorted(directory.listFiles())), compressor);
    }

    /**
     * appends a set of files while only making a single call to {@link org.apache.hadoop.fs.FileSystem#append(Path)}
     * there is no need to call {@link #commit()} after calling this method
     *
     * @param files the files to append
     * @throws IOException if there is an IO problem
     */
    public void batchAppend(final Iterable<File> files) throws IOException {
        batchAppend(files, defaultCompressor);
    }

    public void batchAppend(final Iterable<File> files, final SquallArchiveCompressor compressor) throws IOException {
        batchAppend(files, compressor, newArchivePath());
    }

    private void batchAppend(final Iterable<File> files, final SquallArchiveCompressor compressor, final Path archivePath) throws IOException {
        try( final FSDataOutputStream os = fs.create(archivePath, false) ) {
            for (final File file : files) {
                if (file.isDirectory()) {
                    batchAppendDirectory(os, file, Lists.newArrayList(file.getName()), compressor, archivePath.getName());
                } else {
                    internalAppendFile(os, file, Collections.<String>emptyList(), compressor, archivePath.getName());
                }
            }
            commit();
        }
    }

    private void batchAppendDirectory(final FSDataOutputStream os, final File directory, final List<String> parentDirectories, final SquallArchiveCompressor compressor, final String archiveFilename) throws IOException {
        for (final File file : sorted(directory.listFiles())) {
            if (file.isDirectory()) {
                final List<String> newParentDirectories = Lists.newArrayList(parentDirectories);
                newParentDirectories.add(file.getName());
                batchAppendDirectory(os, file, newParentDirectories, compressor, archiveFilename);
            } else {
                internalAppendFile(os, file, parentDirectories, compressor, archiveFilename);
            }
        }
    }

    /**
     * recursively append a directory to the archive
     * does not modify metadata.txt until {@link #commit()} is called
     *
     * @param directory the directory to append
     * @throws IOException if there is an IO problem
     */
    public void appendDirectory(final File directory) throws IOException {
        appendDirectory(directory, defaultCompressor);
    }

    public void appendDirectory(final File directory, final SquallArchiveCompressor compressor) throws IOException {
        appendDirectory(directory, Collections.<String>emptyList(), compressor);
    }

    private void appendDirectory(final File directory, final List<String> parentDirectories, final SquallArchiveCompressor compressor) throws IOException {
        if (!directory.exists() || !directory.isDirectory()) {
            throw new FileNotFoundException(directory.getAbsolutePath() + " either does not exist or is not a directory");
        }

        final List<String> newParentDirectories = new ArrayList<>(parentDirectories);
        newParentDirectories.add(directory.getName().replaceAll("\\s+", "_"));
        for (final File file : sorted(directory.listFiles())) {
            if (file.isDirectory()) {
                appendDirectory(file, newParentDirectories, compressor);
            } else {
                appendFile(file, newParentDirectories, compressor);
            }
        }
    }

    /**
     * append a file from the local file system into the archive
     * does not modify metadata.txt until {@link #commit()} is called
     *
     * @param file a file on the local file system
     * @throws IOException if the file does not exist or if there is an IO problem
     */
    public void appendFile(final File file) throws IOException {
        appendFile(file, defaultCompressor);
    }

    public void appendFile(final File file, final SquallArchiveCompressor compressor) throws IOException {
        appendFile(file, Collections.<String>emptyList(), compressor);
    }

    private void appendFile(final File file, final List<String> parentDirectories, final SquallArchiveCompressor compressor) throws IOException {
        if (!file.exists() || file.isDirectory()) {
            throw new FileNotFoundException(file.getAbsolutePath() + " either does not exist or is a directory");
        }

        final Path archivePath = newArchivePath();

        try( final FSDataOutputStream os = fs.create(archivePath, false) ) {
            internalAppendFile(os, file, parentDirectories, compressor, archivePath.getName());
        }
    }

    private void internalAppendFile(
            final FSDataOutputStream os,
            final File file,
            final List<String> parentDirectories,
            final SquallArchiveCompressor compressor,
            final String archiveFilename) throws IOException {
        final String baseFilename = file.getName().replaceAll("\\s+", "_");
        final String filename = makeFilename(parentDirectories, baseFilename);
        final long size = file.length();
        final long timestamp = file.lastModified();
        final long startOffset = os.getPos();

        final String checksum;
        try( final InputStream is = new BufferedInputStream(new FileInputStream(file)) ) {
            final CompressionOutputStream cos = compressor.newOutputStream(os);
            final DigestOutputStream dos = new DigestOutputStream(cos, ArchiveUtils.getMD5Digest());
            ByteStreams.copy(is, dos);
            checksum = ArchiveUtils.toHex(dos.getMessageDigest().digest());
            cos.finish();
        }

        pendingMetadataWrites.add(new FileMetadata(filename, size, timestamp, checksum, startOffset, compressor, archiveFilename));
    }

    /**
     * flushes pending metadata writes to metadata.txt
     *
     * @throws IOException if there is an IO problem
     */
    public void commit() throws IOException {
        if (pendingMetadataWrites.isEmpty()) {
            return;
        }

        final Path metadataPath = new Path(path, "metadata.txt");
        final Path tmpMetadataPath = new Path(path, "metadata." + UUID.randomUUID() + ".txt.tmp");
        try( final BufferedReader r = new BufferedReader(new InputStreamReader(fs.open(metadataPath), Charsets.UTF_8));
            final PrintWriter w = new PrintWriter(new OutputStreamWriter(fs.create(tmpMetadataPath, false), Charsets.UTF_8))) {
                for (String line = r.readLine(); line != null; line = r.readLine()) {
                    w.println(line);
                }
                for (final FileMetadata file : pendingMetadataWrites) {
                    w.println(TAB.join(file.getFilename(), file.getSize(), file.getTimestamp(), file.getChecksum(), file.getStartOffset(), file.getCompressor().getKey(), file.getArchiveFilename()));
                }
        }
        fs.delete(metadataPath, false);
        fs.rename(tmpMetadataPath, metadataPath);
        pendingMetadataWrites.clear();
    }

    private static String makeFilename(final List<String> parentDirectories, final String baseFilename) {
        final List<String> stringsToJoin = new ArrayList<>(parentDirectories);
        stringsToJoin.add(baseFilename);
        return SLASH.join(stringsToJoin);
    }

    private static File[] sorted(final File[] files) {
        Arrays.sort(files, new Comparator<File>() {
            @Override
            public int compare(final File o1, final File o2) {
                return o1.getName().compareTo(o2.getName());
            }
        });
        return files;
    }
}
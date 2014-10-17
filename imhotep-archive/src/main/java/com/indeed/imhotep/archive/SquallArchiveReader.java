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
 package com.indeed.imhotep.archive;

import com.google.common.base.Charsets;
import com.indeed.util.io.Files;
import com.indeed.imhotep.archive.compression.SquallArchiveCompressor;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.security.DigestInputStream;
import java.util.ArrayList;
import java.util.List;

/**
 * @author jsgroth
 */
public class SquallArchiveReader {
    private static final Logger log = Logger.getLogger(SquallArchiveReader.class);

    private final FileSystem fs;
    private final Path path;

    /**
     * create a SquallArchiveReader
     *
     * @param fs a file system
     * @param path the directory where the archive is located
     */
    public SquallArchiveReader(FileSystem fs, Path path) {
        this.fs = fs;
        this.path = path;
    }

    /**
     * get a list of all files contained in the metadata for this archive
     *
     * @return a list of file metadata
     * @throws IOException if there is an IO problem
     */
    public List<FileMetadata> readMetadata() throws IOException {
        int retries = 3;
        while (true) {
            try {
                final BufferedReader r = new BufferedReader(new InputStreamReader(fs.open(new Path(path, "metadata.txt")), Charsets.UTF_8));
                try {
                    final List<FileMetadata> ret = new ArrayList<FileMetadata>();
                    for (String line = r.readLine(); line != null; line = r.readLine()) {
                        final FileMetadata metadata = parseMetadata(line);
                        ret.add(metadata);
                    }
                    return ret;
                } finally {
                    r.close();
                }
            } catch (FileNotFoundException e) {
                if (--retries == 0) throw e;
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ie) {
                    throw new RuntimeException(ie);
                }
            }
        }
    }

    private static FileMetadata parseMetadata(String line) throws IOException {
        final String[] split = line.split("\t");
        if (split.length < 5) {
            throw new IOException("malformed metadata line: " + line);
        }
        final String filename = split[0];
        final long size = Long.parseLong(split[1]);
        final long timestamp = Long.parseLong(split[2]);
        final String checksum = split[3];
        final long startOffset = Long.parseLong(split[4]);
        final SquallArchiveCompressor compressor = split.length > 5 ?
                SquallArchiveCompressor.fromKey(split[5]) : SquallArchiveCompressor.NONE;
        final String archiveFilename = split.length > 6 ? split[6] : "archive.bin";
        return new FileMetadata(filename, size, timestamp, checksum, startOffset, compressor, archiveFilename);
    }

    /**
     * copies every file in the archive to a local directory
     *
     * @param localDir the directory to copy files into
     * @throws IOException if there is an IO problem
     */
    public void copyAllToLocal(String localDir) throws IOException {
        copyAllToLocal(new File(localDir), new AcceptAllFileMetadataFilter());
    }

    /**
     * copies every file in the archive to a local directory
     *
     * @param localDir the directory to copy files into
     * @throws IOException if there is an IO problem
     */
    public void copyAllToLocal(File localDir) throws IOException {
        copyAllToLocal(localDir, new AcceptAllFileMetadataFilter());
    }

    /**
     * copies every file in the archive that is accepted by the filter to a local directory
     * @param localDir the directory to copy files into
     * @param filter a function specifying which files should be copied
     * @throws IOException if there is an IO problem
     */
    public void copyAllToLocal(File localDir, FileMetadataFilter filter) throws IOException {
        for (final FileMetadata metadata : readMetadata()) {
            if (filter.accept(metadata)) {
                copyToLocal(metadata, localDir);
            }
        }
    }

    /**
     * copy a file from this archive to a local directory
     * 
     * @param filename the file to copy
     * @param localDir the directory to copy into
     * @throws IOException if the given file is not in the archive or if there is an IO problem
     */
    public void copyToLocal(String filename, String localDir) throws IOException {
        final List<FileMetadata> metadataList = readMetadata();
        for (final FileMetadata metadata : metadataList) {
            if (filename.equals(metadata.getFilename())) {
                copyToLocal(metadata, new File(localDir));
                return;
            }
        }
        throw new FileNotFoundException("this archive does not have a file named " + filename);
    }

    /**
     * copy a file from this archive to a local directory
     *
     * @param file the metadata for the file to copy
     * @param localDir the directory to copy into
     * @throws IOException if there is an IO problem
     */
    public void copyToLocal(FileMetadata file, File localDir) throws IOException {
        int retries = 3;
        while (true) {
            try {
                tryCopyToLocal(file, localDir);
                break;
            } catch (IOException e) {
                log.error(e);
                if (--retries == 0) throw e;
                try {
                    Thread.sleep(10000);
                } catch (InterruptedException ie) {
                    log.error(e);
                }
            }
        }
    }

    public void tryCopyToLocal(FileMetadata file, File localDir) throws IOException {
        if (!localDir.exists() && !localDir.mkdirs()) {
            throw new IOException("could not create directory " + localDir);
        }

        final String fullFilename = file.getFilename();
        final File targetFile;
        if (fullFilename.contains("/")) {
            final int lastSlash = fullFilename.lastIndexOf('/');
            final String[] parentDirs = fullFilename.substring(0, lastSlash).split("/");
            final String fullParentPath = Files.buildPath(parentDirs);
            final File parentFile = new File(localDir, fullParentPath);
            if ((!parentFile.exists() && !parentFile.mkdirs()) || (parentFile.exists() && !parentFile.isDirectory())) {
                throw new IOException("unable to create directory " + parentFile.getAbsolutePath());
            }
            targetFile = new File(parentFile, fullFilename.substring(lastSlash + 1));
        } else {
            targetFile = new File(localDir, file.getFilename());
        }

        final Path archivePath = new Path(path, file.getArchiveFilename());
        final SquallArchiveCompressor compressor = file.getCompressor();
        final FSDataInputStream is = fs.open(archivePath);
        try {
            is.seek(file.getStartOffset());
            final DigestInputStream digestStream = new DigestInputStream(compressor.newInputStream(is), ArchiveUtils.getMD5Digest());
            final OutputStream os = new BufferedOutputStream(new FileOutputStream(targetFile));
            ArchiveUtils.streamCopy(digestStream, os, file.getSize());
            os.close();
            final String checksum = ArchiveUtils.toHex(digestStream.getMessageDigest().digest());
            if (!checksum.equals(file.getChecksum())) {
                throw new IOException("invalid checksum for file " + fullFilename + " in archive " + path + ": file checksum = " + checksum + ", checksum in metadata = " + file.getChecksum());
            }
        } finally {
            is.close();
        }
    }
}

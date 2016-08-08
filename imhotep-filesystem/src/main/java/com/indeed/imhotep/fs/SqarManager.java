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
 package com.indeed.imhotep.fs;

import com.almworks.sqlite4java.*;
import com.google.common.base.Charsets;
import com.google.common.io.ByteStreams;
import com.indeed.imhotep.archive.ArchiveUtils;
import com.indeed.imhotep.archive.FileMetadata;
import com.indeed.imhotep.archive.compression.SquallArchiveCompressor;
import com.indeed.imhotep.fs.sqlite.AddNewSqarJob;
import com.indeed.imhotep.fs.sqlite.InitDBJob;
import com.indeed.imhotep.fs.sqlite.LookupSqarIdJob;
import com.indeed.imhotep.fs.sqlite.ReadPathInfoJob;
import com.indeed.imhotep.fs.sqlite.ScanSqarDirJob;
import org.apache.log4j.Logger;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.DigestInputStream;
import java.util.*;

public class SqarManager {
    private static final Logger log = Logger.getLogger(SqarManager.class);
    public static final char DELIMITER = '/';
    private static final int DEFAULT_MAX_MEM_USAGE = 10 * 1024 * 1024;  // 10MB

    private final SQLiteQueue queue;

    static {
        try {
            final String osName = System.getProperty("os.name");
            final String arch = System.getProperty("os.arch");
            final String libname = "libsqlite4java"
                    + "-" + osName.toLowerCase()
                    + "-" + arch.toLowerCase()
                    + "-1.0.392.so";
            final String resourcePath = "/native/" + osName + "-" + arch + "/" + libname;
            final InputStream is = SqarManager.class.getResourceAsStream(resourcePath);
            if (is == null) {
                throw new FileNotFoundException(
                        "unable to find sqlite4java-linux-amd64-1.0.392 "
                                + "at resource path " + resourcePath);
            }
            final File tempFile = File.createTempFile("libsqlite", ".so");
            final File libFile = new File(tempFile.getParent(), libname);
            tempFile.delete();
            final OutputStream os = new FileOutputStream(libFile);
            ByteStreams.copy(is, os);
            os.close();
            is.close();
            SQLite.setLibraryPath(libFile.getParent());
            libFile.deleteOnExit();
        } catch (Throwable e) {
            e.printStackTrace();
            log.warn("Unable to copy libsqlite to tmp file, "
                             + "will look in java.library.path", e);
        }
    }


    public SqarManager(Map<String, String> settings) {
        final int maxMemUsage;
        final String maxMem = settings.get("sqlite-max-mem");

        if (maxMem != null) {
            maxMemUsage = Integer.parseInt(maxMem);
        } else {
            maxMemUsage = DEFAULT_MAX_MEM_USAGE;
        }

        try {
            SQLite.setSoftHeapLimit(maxMemUsage);
        } catch (final SQLiteException e) {
            throw new IllegalArgumentException("Failed to set SQLite softHeapLimit to " + maxMemUsage, e);
        }

        final File dbFile = new File(settings.get("database-location"));

        this.queue = new SQLiteQueue(dbFile);
        this.queue.start();

        /* initialized DB */
        final InitDBJob initJob = new InitDBJob();
        queue.execute(initJob).complete();
    }

    public static boolean isSqar(RemoteCachingPath path, RemoteFileStore fs) throws IOException {
        final SqarPathInfo info = new SqarPathInfo(path);
        final RemoteFileStore.RemoteFileInfo rfi;

        if (info.sqarDir == null) {
            return false;
        }
        rfi = fs.readInfo(info.sqarDir, false);
        return rfi != null && rfi.isDirectory();
    }

    public static boolean isSqarDir(String dirName) throws IOException {
        return dirName != null && dirName.endsWith(SqarPathInfo.SUFFIX);
    }

    public static String decodeShardName(String pathStr) {
        return pathStr.substring(0, pathStr.length() - SqarPathInfo.SUFFIX.length());
    }

    private static FileMetadata parseMetadataLine(String line) throws IOException {
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
        return new FileMetadata(filename, size, timestamp, checksum, startOffset, compressor,
                                archiveFilename);
    }

    private ArrayList<FileMetadata> parseMetadataFile(InputStream metadataIS) throws IOException {
        final ArrayList<FileMetadata> fileList = new ArrayList<>(1024);

        /* parse file */
        try (BufferedReader r = new BufferedReader(new InputStreamReader(metadataIS,
                                                                         Charsets.UTF_8))) {
            for (String line = r.readLine(); line != null; line = r.readLine()) {
                fileList.add(parseMetadataLine(line));
            }
        }

        /* ensure the list is sorted by archive and file position */
        Collections.sort(fileList, new Comparator<FileMetadata>() {
            @Override
            public int compare(FileMetadata fmd1, FileMetadata fmd2) {
                final String fmd1Archive = fmd1.getArchiveFilename();
                final String fmd2Archive = fmd2.getArchiveFilename();

                final int cmpResult = fmd1Archive.compareTo(fmd2Archive);
                if (cmpResult != 0) {
                    return cmpResult;
                }

                final long fmd1Offset = fmd1.getStartOffset();
                final long fmd2Offset = fmd2.getStartOffset();
                return Long.compare(fmd1Offset, fmd2Offset);
            }
        });

        FileMetadata prevFmd = fileList.get(0);
        long prevOffset = prevFmd.getStartOffset();
        for (int i = 1; i < fileList.size(); i++) {
            final FileMetadata fmd = fileList.get(i);
            final long offset = fmd.getStartOffset();
            if (offset == 0) {
                /* changed archive files */
                prevFmd.setCompressedSize(-1);
            } else {
                prevFmd.setCompressedSize(offset - prevOffset);
            }
            prevFmd = fmd;
            prevOffset = offset;
        }
        prevFmd.setCompressedSize(-1);
        return fileList;
    }

    public Integer cacheMetadata(RemoteCachingPath shard, InputStream metadataIS) throws
                                                                                  IOException {
        final ArrayList<FileMetadata> fileList = parseMetadataFile(metadataIS);
        final HashSet<String> dirList = new HashSet<>();

        /* identify directories */
        for (FileMetadata md : fileList) {
            final String fname = md.getFilename();
            final int loc = fname.lastIndexOf(DELIMITER);

            if (loc <= 0) {
                /* not a directory if the only delimiter is the first char
                 * or does not have a delimiter at all
                 */
                continue;
            }
            for (int i = 0; i < loc; i++) {
                if (fname.charAt(i) == DELIMITER) {
                    dirList.add(fname.substring(0, i));
                }
            }
            dirList.add(fname.substring(0, loc));
        }
        /* add directories */
        for (String dir : dirList) {
            final FileMetadata md = new FileMetadata(dir, false);
            fileList.add(md);
        }

        final AddNewSqarJob addNewSqarJob = new AddNewSqarJob(shard.getShardPath(), fileList);
        return queue.execute(addNewSqarJob).complete();
    }

    public PathInfoResult getPathInfo(RemoteCachingPath path) throws IOException {
        final FileMetadata resultMetadata;

        /* check to see if the sqar's metadata has been downloaded before */
        final LookupSqarIdJob lookupSqarIdJob = new LookupSqarIdJob(path.getShardPath());
        final int sqarId = queue.execute(lookupSqarIdJob).complete();
        if (sqarId == -1) {
            return PathInfoResult.ARCHIVE_MISSING;
        }

        /* top level shard directory is not listed in db,
         * so we need to check for that case special
         */
        if (ImhotepPathType.SHARD.equals(path.getType())) {
            resultMetadata = new FileMetadata("", false);
            return new PathInfoResult(resultMetadata);
        }

        /* find the info about this compressed file */
        final ReadPathInfoJob readPathInfoJob = new ReadPathInfoJob(sqarId, path.getFilePath());
        resultMetadata = queue.execute(readPathInfoJob).complete();
        if (resultMetadata == null) {
            return PathInfoResult.FILE_MISSING;
        } else {
            return new PathInfoResult(resultMetadata);
        }
    }

    public static final class PathInfoResult {
        public static final PathInfoResult ARCHIVE_MISSING = new PathInfoResult(null);
        public static final PathInfoResult FILE_MISSING = new PathInfoResult(null);

        public final FileMetadata metadata;

        public PathInfoResult(FileMetadata metadata) {
            this.metadata = metadata;
        }
    }

    public String getFullArchivePath(RemoteCachingPath path, String archiveFile) {
        final SqarPathInfo pathInfo = new SqarPathInfo(path);

        return pathInfo.sqarDir + DELIMITER + archiveFile;
    }

    public void copyDecompressed(InputStream is,
                                 Path localFile,
                                 FileMetadata metadata,
                                 String fullPath) throws IOException {
        final SquallArchiveCompressor compressor = metadata.getCompressor();
        final long originalSize = metadata.getSize();
        final DigestInputStream digestStream;
        final OutputStream os;

        digestStream = new DigestInputStream(compressor.newInputStream(is),
                                             ArchiveUtils.getMD5Digest());
        os = new BufferedOutputStream(Files.newOutputStream(localFile));
        ArchiveUtils.streamCopy(digestStream, os, originalSize);
        os.close();

        final String checksum = ArchiveUtils.toHex(digestStream.getMessageDigest().digest());
        if (!checksum.equals(metadata.getChecksum())) {
            throw new IOException("invalid checksum for file " + fullPath +
                                  ": file checksum = " + checksum +
                                  ", checksum in metadata = " + metadata.getChecksum());
        }
    }

    public ArrayList<RemoteFileStore.RemoteFileInfo> readDir(RemoteCachingPath path) {
        /* check to see if the sqar's metadata has been downloaded before */
        final LookupSqarIdJob lookupSqarIdJob = new LookupSqarIdJob(path.getShardPath());
        final int sqarId = queue.execute(lookupSqarIdJob).complete();
        if (sqarId == -1) {
            return null;
        }

        /* find the info about this compressed file */
        final ScanSqarDirJob sqarDirJob = new ScanSqarDirJob(sqarId, path.getFilePath());
        return queue.execute(sqarDirJob).complete();
    }

    public static String getMetadataLoc(RemoteCachingPath path) {
        return new SqarPathInfo(path).metadataPath;
    }

    static class SqarPathInfo {
        private static final String SUFFIX = ".sqar";
        private static final String METADATA_FILE = "metadata.txt";

        String sqarDir;
        String metadataPath;

        private SqarPathInfo(RemoteCachingPath path) {
            if (path.getNameCount() >= 2) {
                sqarDir = path.getShardPath() + SUFFIX;
                metadataPath = sqarDir + DELIMITER + METADATA_FILE;
            }
        }
    }
}

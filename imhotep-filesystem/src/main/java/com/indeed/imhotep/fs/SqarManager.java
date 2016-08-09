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

import com.google.common.base.Charsets;
import com.indeed.imhotep.archive.ArchiveUtils;
import com.indeed.imhotep.archive.FileMetadata;
import com.indeed.imhotep.archive.compression.SquallArchiveCompressor;
import org.apache.log4j.Logger;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.DigestInputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.*;

public class SqarManager {
    private static final Logger log = Logger.getLogger(SqarManager.class);
    private static final int DEFAULT_MAX_MEM_USAGE = 10 * 1024 * 1024;  // 10MB
    public static final char DELIMITER = '/';

    private final Connection connection;
    private final SqlStatements sqlStatements;

    public SqarManager(Map<String, String> settings) throws  ClassNotFoundException, SQLException {
        final int maxMemUsage;
        final String maxMem = settings.get("sqlite-max-mem");

        if (maxMem != null) {
            maxMemUsage = Integer.parseInt(maxMem);
        } else {
            maxMemUsage = DEFAULT_MAX_MEM_USAGE;
        }

        final File dbFile = new File(settings.get("database-location"));

        /* initialize DB */
        Class.forName("org.h2.Driver");
        final String dbSettings = ";CACHE_SIZE=" + maxMemUsage;
        this.connection = DriverManager.getConnection("jdbc:h2:" + dbFile + dbSettings);

        /* initialize DB */
        SqlStatements.execute_CREATE_SQAR_TBL(connection);
        SqlStatements.execute_CREATE_SQAR_IDX(connection);
        SqlStatements.execute_CREATE_FILE_NAMES_TBL(connection);
        SqlStatements.execute_CREATE_FILE_NAMES_IDX(connection);
        SqlStatements.execute_CREATE_FILE_IDS_TBL(connection);
        SqlStatements.execute_CREATE_FILE_IDS_IDX(connection);
        SqlStatements.execute_CREATE_FILENAME_2_ID_VIEW(connection);
        SqlStatements.execute_CREATE_ARCHIVE_TBL(connection);
        SqlStatements.execute_CREATE_ARCHIVE_IDX(connection);
        SqlStatements.execute_CREATE_FILE_INFO_TBL(connection);
//        SqlStatements.execute_CREATE_FILE_INFO_IDX(connection);
        this.sqlStatements = new SqlStatements(connection);

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

    public void cacheMetadata(RemoteCachingPath shard, InputStream metadataIS) throws IOException {
        try {
            final int sqarId = getShardId(shard.getShardPath());
            cacheMetadataInternal(sqarId, metadataIS);
        } catch (SQLException e) {
            throw new IOException(e);
        }
    }

    public void cacheMetadataInternal(int shardId, InputStream metadataIS) throws IOException {
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

        try {
            addFilesFromSqar(shardId, fileList);
        } catch (SQLException e) {
            throw new IOException(e);
        }
    }

    public PathInfoResult getPathInfo(RemoteCachingPath path) throws IOException {
        try {
            final FileMetadata resultMetadata;

        /* check to see if the sqar's metadata has been downloaded before */
            final int sqarId;
            sqarId = this.sqlStatements.execute_SELECT_SQAR_ID_STATEMENT(path.getShardPath());
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
            resultMetadata = this.sqlStatements.execute_SELECT_FILE_INFO_STATEMENT(sqarId,
                                                                                   path.getFilePath());
            if (resultMetadata == null) {
                return PathInfoResult.FILE_MISSING;
            } else {
                return new PathInfoResult(resultMetadata);
            }
        } catch (SQLException e) {
            throw new IOException(e);
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

    public ArrayList<RemoteFileStore.RemoteFileInfo>
    readDir(RemoteCachingPath path) throws IOException {
        try {
            /* check to see if the sqar's metadata has been downloaded before */
            final int sqarId = this.sqlStatements.execute_SELECT_SQAR_ID_STATEMENT(path.getShardPath());
            if (sqarId == -1) {
                return null;
            }

            /* find the info about this compressed file */
            ArrayList<SqlStatements.FileOrDir> foo = listFilesAndDirsUnderPrefix(sqarId, path.getFilePath());
            ArrayList<RemoteFileStore.RemoteFileInfo> results = new ArrayList<>(foo.size());
            for (SqlStatements.FileOrDir f : foo) {
                results.add(new RemoteFileStore.RemoteFileInfo(f.name, -1, f.isFile));
            }
            return results;
        } catch (SQLException e) {
            throw new IOException(e);
        }
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

    private void addFilesFromSqar(int sqarId, List<FileMetadata> metadata) throws
            IOException,
            SQLException {

        for (FileMetadata md : metadata) {
            final int filenameId = getFilenameId(md.getFilename());
            final int fileId = getFileId(sqarId, filenameId, md.isFile());
            final int archiveId;
            if (md.isFile()) {
                archiveId = getArchiveId(md.getArchiveFilename());
            } else {
                archiveId = -1;
            }
            insertIntoFileInfo(fileId,
                               archiveId,
                               md.getStartOffset(),
                               md.getSize(),
                               md.getCompressedSize(),
                               md.getTimestamp(),
                               md.getChecksumHi(),
                               md.getChecksumLow(),
                               md.getCompressor(),
                               md.isFile());
        }
    }

    private int getArchiveId(String name) throws IOException, SQLException {
        int id;
        SQLException savedException = null;

        for (int i = 0; i < 3; i++) {
            id = this.sqlStatements.execute_SELECT_ARCHIVE_ID_STATEMENT(name);
            if (id != -1) {
                return id;
            } else {
                try {
                    id = this.sqlStatements.execute_INSERT_ARCHIVE_NAME_STATEMENT(name);
                } catch (SQLException e) {
                    savedException = e;
                    continue;
                }
                if (id != -1) {
                    return id;
                } else {
                    throw new IOException("Shard missing after insert.");
                }
            }
        }
        /* if we get here, this should be true */
        if (savedException != null) {
            throw savedException;
        }
        return -1;
    }

    private int getShardId(String name) throws IOException, SQLException {
        int id;
        SQLException savedException = null;

        for (int i = 0; i < 3; i++) {
            id = this.sqlStatements.execute_SELECT_SHARD_ID_STATEMENT(name);
            if (id != -1) {
                return id;
            } else {
                try {
                    id = this.sqlStatements.execute_INSERT_SHARD_NAME_STATEMENT(name);
                } catch (SQLException e) {
                    savedException = e;
                    continue;
                }
                if (id != -1) {
                    return id;
                } else {
                    throw new IOException("Shard missing after insert.");
                }
            }
        }
        /* if we get here, this should be true */
        if (savedException != null) {
            throw new RuntimeException(savedException);
        }
        return -1;
    }

    private int getFilenameId(String filename) throws IOException, SQLException {
        int id;
        SQLException savedException = null;

        for (int i = 0; i < 3; i++) {
            id = this.sqlStatements.execute_SELECT_FILE_NAME_ID_STATEMENT(filename);
            if (id != -1) {
                return id;
            } else {
                try {
                    id = this.sqlStatements.execute_INSERT_FILE_NAME_STATEMENT(filename);
                } catch (SQLException e) {
                    savedException = e;
                    continue;
                }
                if (id != -1) {
                    return id;
                } else {
                    throw new IOException("Shard missing after insert.");
                }
            }
        }
        /* if we get here, this should be true */
        if (savedException != null) {
            throw new RuntimeException(savedException);
        }
        return -1;
    }

    private int getFileId(int sqarId, int filenameId, boolean isFile) throws
            IOException,
            SQLException {
        int id;
        SQLException savedException = null;

        for (int i = 0; i < 3; i++) {
            id = this.sqlStatements.execute_SELECT_FILE_ID_STATEMENT(sqarId, filenameId);
            if (id != -1) {
                return id;
            } else {
                try {
                    id = this.sqlStatements.execute_INSERT_FILE_JOIN_VALUE_STATEMENT(sqarId,
                                                                                     filenameId,
                                                                                     isFile);
                } catch (SQLException e) {
                    savedException = e;
                    continue;
                }
                if (id != -1) {
                    return id;
                } else {
                    throw new IOException("Shard missing after insert.");
                }
            }
        }
        /* if we get here, this should be true */
        if (savedException != null) {
            throw new RuntimeException(savedException);
        }
        return -1;
    }

    private void insertIntoFileInfo(int fileId,
                                    int archiveId,
                                    long archiveOffset,
                                    long unpackedSize,
                                    long packedSize,
                                    long timestamp,
                                    long sigHi,
                                    long sigLow,
                                    SquallArchiveCompressor compressor,
                                    boolean isFile) throws SQLException {
        this.sqlStatements.execute_INSERT_FILE_INFO_STATEMENT(fileId,
                                                         archiveId,
                                                         archiveOffset,
                                                         unpackedSize,
                                                         packedSize,
                                                         timestamp,
                                                         sigHi,
                                                         sigLow,
                                                         compressor.getKey(),
                                                         isFile);
    }

    /* basically list a directory, where the "prefix" is the directory to list */
    private ArrayList<SqlStatements.FileOrDir>
    listFilesAndDirsUnderPrefix(int sqarId, String prefix) throws SQLException {
        if (prefix == null || prefix.isEmpty()) {
            return this.sqlStatements.execute_SELECT_FILE_NAMES_NO_PREFIX_STATEMENT(sqarId);
        }

        // add delimiter to end of prefix if it is not already there
        if (prefix.charAt(prefix.length() - 1) != SqarManager.DELIMITER) {
            prefix = prefix + SqarManager.DELIMITER;
        }
        return this.sqlStatements.execute_SELECT_FILE_NAMES_WITH_PREFIX_STATEMENT(sqarId, prefix);
    }
}

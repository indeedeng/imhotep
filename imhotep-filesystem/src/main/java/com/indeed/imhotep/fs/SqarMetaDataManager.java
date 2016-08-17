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
import com.google.common.base.Function;
import com.google.common.collect.FluentIterable;
import com.indeed.imhotep.archive.ArchiveUtils;
import com.indeed.imhotep.archive.FileMetadata;
import com.indeed.imhotep.archive.compression.SquallArchiveCompressor;
import org.apache.log4j.Logger;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.Closeable;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.security.DigestInputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class SqarMetaDataManager implements Closeable {
    private static final Logger LOGGER = Logger.getLogger(SqarMetaDataManager.class);
    static final char DELIMITER = '/';

    private final Connection connection;
    private final SqlStatements sqlStatements;

    public SqarMetaDataManager(final Map<String, ?> settings) throws ClassNotFoundException, SQLException {
        final File dbFile = new File((String) settings.get("imhotep.fs.sqardb.path"));

        Class.forName("org.h2.Driver");
        connection = DriverManager.getConnection("jdbc:h2:" + dbFile);

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
        sqlStatements = new SqlStatements(connection);

    }

    @Override
    public void close() throws IOException {
        try {
            connection.close();
        } catch (final SQLException e) {
            LOGGER.warn("Failed to close connection", e);
        }
    }

    /**
     * true if the contents is within a 'sqar' directory
     */
    static boolean isInSqarDirectory(final RemoteCachingPath path, final RemoteFileStore fs) throws IOException {
        final SqarPathInfo info = new SqarPathInfo(path);

        if (info.sqarDir == null) {
            return false;
        }

        final RemoteFileStore.RemoteFileAttributes rfi;
        try {
            rfi = fs.getRemoteAttributes(info.sqarDir, false);
        } catch (final NoSuchFileException|FileNotFoundException e) {
            return false;
        }
        return rfi.isDirectory();
    }

    private static FileMetadata parseMetadataLine(final String line) throws IOException {
        final String[] split = line.split("\t");

        if (split.length < 5) {
            throw new IOException("malformed metadata line: " + line);
        }
        final String filename = split[0];
        final long size = Long.parseLong(split[1]);
        final long timestamp = Long.parseLong(split[2]);
        final String checksum = split[3];
        final long startOffset = Long.parseLong(split[4]);
        final SquallArchiveCompressor compressor = (split.length > 5) ?
                SquallArchiveCompressor.fromKey(split[5]) : SquallArchiveCompressor.NONE;
        final String archiveFilename = (split.length > 6) ? split[6] : "archive.bin";
        return new FileMetadata(filename, size, timestamp, checksum, startOffset, compressor,
                archiveFilename);
    }

    private static List<FileMetadata> parseMetadataFile(final InputStream metadataIS) throws IOException {
        final List<FileMetadata> fileList = new ArrayList<>(1024);

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

    void cacheMetadata(final RemoteCachingPath path, final InputStream metadataIS) throws IOException {
        try {
            final int sqarId = getOrCreateShardId(path.getShardPath().toString());
            cacheMetadata(sqarId, metadataIS);
        } catch (final SQLException e) {
            throw new IOException(e);
        }
    }

    public void cacheMetadata(final int shardId, final InputStream metadataIS) throws IOException {
        final List<FileMetadata> fileList = parseMetadataFile(metadataIS);
        final Set<String> dirList = new HashSet<>();

        /* identify directories */
        for (final FileMetadata md : fileList) {
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
        for (final String dir : dirList) {
            final FileMetadata md = new FileMetadata(dir, false);
            fileList.add(md);
        }

        try {
            addFilesFromSqar(shardId, fileList);
        } catch (final SQLException e) {
            throw new IOException(e);
        }
    }

    PathInfoResult getPathInfo(final RemoteCachingPath path) throws IOException {
        try {
            // check to see if the sqar's metadata has been downloaded before
            final int sqarId;
            sqarId = this.sqlStatements.execute_SELECT_SQAR_ID_STATEMENT(path.getShardPath().toString());
            if (sqarId == -1) {
                return PathInfoResult.ARCHIVE_MISSING;
            }

            // top level shard directory is not listed in db,
            // so we need to check for that case special
            if (ImhotepPathType.SHARD.equals(path.getType())) {
                return new PathInfoResult(new FileMetadata("", false));
            }

            // find the info about this compressed file
            final FileMetadata resultMetadata = this.sqlStatements.execute_SELECT_FILE_INFO_STATEMENT(sqarId,
                    path.getFilePath().toString());
            if (resultMetadata == null) {
                return PathInfoResult.FILE_MISSING;
            } else {
                return new PathInfoResult(resultMetadata);
            }
        } catch (final SQLException e) {
            throw new IOException("Failed to get path info for " + path, e);
        }
    }

    static final class PathInfoResult {
        static final PathInfoResult ARCHIVE_MISSING = new PathInfoResult(null);
        static final PathInfoResult FILE_MISSING = new PathInfoResult(null);

        final FileMetadata metadata;

        PathInfoResult(final FileMetadata metadata) {
            this.metadata = metadata;
        }
    }

    static RemoteCachingPath getFullArchivePath(final RemoteCachingPath path, final String archiveFile) {
        return new SqarPathInfo(path).sqarDir.resolve(archiveFile);
    }

    void copyDecompressed(final InputStream is,
                          final RemoteCachingPath srcPath,
                          final Path destPath,
                          final FileMetadata metadata) throws IOException {
        final Path destParent = destPath.getParent();
        if (destParent != null) {
            Files.createDirectories(destParent);
        }
        final String checksum;
        try (DigestInputStream digestStream = new DigestInputStream(metadata.getCompressor().newInputStream(is),
                ArchiveUtils.getMD5Digest())) {

            try (OutputStream os = new BufferedOutputStream(Files.newOutputStream(destPath))) {
                ArchiveUtils.streamCopy(digestStream, os, metadata.getSize());
            }

            checksum = ArchiveUtils.toHex(digestStream.getMessageDigest().digest());
        }

        if (!checksum.equals(metadata.getChecksum())) {
            throw new IOException("invalid checksum for file " + srcPath +
                    ": file checksum = " + checksum +
                    ", checksum in metadata = " + metadata.getChecksum());
        }
    }

    List<RemoteFileStore.RemoteFileAttributes>
    readDir(final RemoteCachingPath path) throws IOException {
        try {
            // check to see if the sqar's metadata has been downloaded before
            final int sqarId = sqlStatements.execute_SELECT_SQAR_ID_STATEMENT(path.getShardPath().toString());
            if (sqarId == -1) {
                return Collections.emptyList();
            }

            final RemoteCachingPath shardFileName = path.getFilePath();
            return FluentIterable.from(listFilesAndDirsUnderPrefix(sqarId, (shardFileName == null) ? "" : shardFileName.toString()))
                    .transform(new Function<SqlStatements.FileOrDir, RemoteFileStore.RemoteFileAttributes>() {
                        @Override
                        public RemoteFileStore.RemoteFileAttributes apply(final SqlStatements.FileOrDir fileOrDir) {
                            return new RemoteFileStore.RemoteFileAttributes(path.resolve(fileOrDir.name), -1, fileOrDir.isFile);
                        }
                    }).toList();
        } catch (final SQLException e) {
            throw new IOException("Failed to retrieve meta data for " + path, e);
        }
    }

    static RemoteCachingPath getMetadataPath(final RemoteCachingPath path) {
        return new SqarPathInfo(path).metadataPath;
    }

    private static class SqarPathInfo {
        private static final String SUFFIX = ".sqar";
        private static final String METADATA_FILE = "metadata.txt";

        RemoteCachingPath sqarDir;
        RemoteCachingPath metadataPath;

        private SqarPathInfo(final RemoteCachingPath path) {
            if (path.getNameCount() >= 2) {
                final RemoteCachingPath shardPath = path.getShardPath();
                if (!shardPath.getFileName().toString().endsWith(SUFFIX)) {
                    sqarDir = ((RemoteCachingPath) shardPath.getParent()).resolve(shardPath.getFileName() + SUFFIX);
                } else {
                    sqarDir = shardPath;
                }
                metadataPath = sqarDir.resolve(METADATA_FILE);
            } else {
                sqarDir = null;
                metadataPath = null;
            }
        }
    }

    private void addFilesFromSqar(final int sqarId, final List<FileMetadata> metadata) throws
            IOException,
            SQLException {

        for (final FileMetadata md : metadata) {
            final int filenameId = getOrCreateFilenameId(md.getFilename());
            final int fileId = getOrCreateFileId(sqarId, filenameId, md.isFile());
            final int archiveId;
            if (md.isFile()) {
                archiveId = getOrCreateArchiveId(md.getArchiveFilename());
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

    private int getOrCreateArchiveId(String name) throws IOException, SQLException {
        int id;
        SQLException savedException = null;

        for (int i = 0; i < 3; i++) {
            id = this.sqlStatements.execute_SELECT_ARCHIVE_ID_STATEMENT(name);
            if (id != -1) {
                return id;
            } else {
                try {
                    id = this.sqlStatements.execute_INSERT_ARCHIVE_NAME_STATEMENT(name);
                } catch (final SQLException e) {
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

    private int getOrCreateShardId(final String name) throws IOException, SQLException {
        int id;
        SQLException savedException = null;

        for (int i = 0; i < 3; i++) {
            id = this.sqlStatements.execute_SELECT_SHARD_ID_STATEMENT(name);
            if (id != -1) {
                return id;
            } else {
                try {
                    id = this.sqlStatements.execute_INSERT_SHARD_NAME_STATEMENT(name);
                } catch (final SQLException e) {
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

    private int getOrCreateFilenameId(String filename) throws IOException, SQLException {
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
            throw savedException;
        }
        return -1;
    }

    private int getOrCreateFileId(int sqarId, int filenameId, boolean isFile) throws
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
            throw savedException;
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
    private List<SqlStatements.FileOrDir>
    listFilesAndDirsUnderPrefix(final int sqarId, String prefix) throws SQLException {
        // add delimiter to end of prefix if it is not already there
        if (!prefix.isEmpty() && (prefix.charAt(prefix.length() - 1) != SqarMetaDataManager.DELIMITER)) {
            prefix += SqarMetaDataManager.DELIMITER;
        }
        return sqlStatements.execute_SELECT_FILE_NAMES_WITH_PREFIX_STATEMENT(sqarId, prefix);
    }
}

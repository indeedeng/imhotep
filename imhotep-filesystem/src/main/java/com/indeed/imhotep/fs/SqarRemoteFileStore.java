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

package com.indeed.imhotep.fs;

import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Lists;
import com.indeed.imhotep.archive.FileMetadata;
import com.indeed.imhotep.fs.db.metadata.tables.records.TblfilemetadataRecord;
import com.indeed.imhotep.fs.sql.FileMetadataDao;
import com.indeed.imhotep.fs.sql.SqarMetaDataDao;
import com.indeed.imhotep.scheduling.TaskScheduler;
import com.indeed.util.core.io.Closeables2;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.apache.log4j.Logger;
import org.joda.time.Period;
import org.jooq.Cursor;
import org.jooq.Result;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.NotDirectoryException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;

/**
 * @author kenh
 */
class SqarRemoteFileStore extends RemoteFileStore implements Closeable {
    private static final Logger log = Logger.getLogger(SqarRemoteFileStore.class);

    private final SqarMetaDataManager sqarMetaDataManager;
    private final SqarMetaDataDao sqarMetaDataDao;
    private final RemoteFileStore backingFileStore;

    SqarRemoteFileStore(final RemoteFileStore backingFileStore,
                               final Map<String, ?> configuration) throws SQLException, IOException {
        this.backingFileStore = backingFileStore;

        final String h2dbFilePath = (String) configuration.get("imhotep.fs.sqardb.file");
        final File h2DBFile = h2dbFilePath != null ? new File(h2dbFilePath) : null;
        final File lsmTreeMetadataStore = new File((String)configuration.get("imhotep.fs.sqar.metadata.cache.path"));
        final String lsmTreeExpirationDurationString = (String)(configuration.get("imhotep.fs.sqar.metadata.cache.expiration.hours"));
        final int lsmTreeExpirationDurationHours = lsmTreeExpirationDurationString != null ? Integer.valueOf(lsmTreeExpirationDurationString) : 0;
        final Duration lsmTreeExpirationDuration = lsmTreeExpirationDurationHours > 0 ? Duration.of(lsmTreeExpirationDurationHours, ChronoUnit.HOURS) : null;

        final boolean h2ToLSMConversionRequired = h2DBFile != null && new File(h2DBFile.toString() + ".mv.db").exists() &&
                !SqarMetaDataLSMStore.lsmTreeExistsInDir(lsmTreeMetadataStore);

        sqarMetaDataDao = new SqarMetaDataLSMStore(lsmTreeMetadataStore, lsmTreeExpirationDuration);

        if(h2ToLSMConversionRequired) {
            try {
                convertBackingStoreFromH2ToLSM(h2DBFile, sqarMetaDataDao);
            } catch (Exception e) {
                log.error("Failed to convert remote FS metadata cache from H2 DB to LSM tree. Exiting to avoid data loss", e);
                System.exit(-1);
            }
        }

        sqarMetaDataManager = new SqarMetaDataManager(sqarMetaDataDao);
    }

    private void convertBackingStoreFromH2ToLSM(File h2DBFile, SqarMetaDataDao lsmMetadataCache) throws IOException, SQLException {
        log.info("Starting one time conversion of remote FS metadata cache from H2 DB to LSM tree");
        final HikariConfig config = new HikariConfig();
        config.setJdbcUrl("jdbc:h2:" + h2DBFile + ";LAZY_QUERY_EXECUTION=1;ACCESS_MODE_DATA=r");
        // TODO remove
        //"jdbc:h2:/tmp/prodsquarmetadata;LAZY_QUERY_EXECUTION=1;ACCESS_MODE_DATA=r")
        final long startTime = System.currentTimeMillis();

        try(final HikariDataSource dataSource = new HikariDataSource(config)) {
            final FileMetadataDao h2MetadataCache = new FileMetadataDao(dataSource);
            final long totalRecords = h2MetadataCache.getNumberOfRecords();
            log.info("Converting " + totalRecords + " records");
            // read everything from H2 DB
            final Cursor<TblfilemetadataRecord> h2DBCursor = h2MetadataCache.getAllRecords();
            log.info("Loaded cursor from H2");
            String currentShardPath = null;
            List<RemoteFileMetadata> currentShardFiles = Lists.newArrayList();
            int convertedRecords = 0;
            // group by shard path and write to LSM tree
            while (h2DBCursor.hasNext()) {
                final Result<TblfilemetadataRecord> h2DBRecords = h2DBCursor.fetch(100000);
                for(final TblfilemetadataRecord h2DBRecord : h2DBRecords) {
                    final String shardPath = h2DBRecord.getShardName();
                    if (!shardPath.equals(currentShardPath)) {
                        if (currentShardPath != null) {
                            lsmMetadataCache.cacheMetadata(Paths.get(currentShardPath), currentShardFiles);
                        }
                        currentShardPath = shardPath;
                        currentShardFiles.clear();
                    }
                    currentShardFiles.add(FileMetadataDao.toRemoteFileMetadata(h2DBRecord));
                    convertedRecords++;
                }
                log.info("Converted " + convertedRecords + " (" + ((long)convertedRecords * 100 / totalRecords) + "%)");
            }
            if(currentShardPath != null) {
                lsmMetadataCache.cacheMetadata(Paths.get(currentShardPath), currentShardFiles);
            }
            h2DBCursor.close();
        }

        log.info("Conversion completed in " + new Period(System.currentTimeMillis() - startTime));
    }

    @Override
    public void close() throws IOException {
        Closeables2.closeQuietly(sqarMetaDataDao, log);
    }

    RemoteFileStore getBackingFileStore() {
        return backingFileStore;
    }

    @Override
    InputStream newInputStream(final RemoteCachingPath path, final long startOffset, final long length) throws IOException {
        return backingFileStore.newInputStream(path, startOffset, length);
    }

    @Override
    public String name() {
        return backingFileStore.name();
    }

    @Override
    List<RemoteFileStore.RemoteFileAttributes> listDir(final RemoteCachingPath path) throws IOException {
        if (isInSqarDirectory(path)) {
            final RemoteFileMetadata sqarMetadata = getSqarMetadata(path);
            if (sqarMetadata == null) {
                throw new NoSuchFileException(path.toString());
            }
            if (sqarMetadata.isFile()) {
                throw new NotDirectoryException(path.toString());
            }
            return sqarMetaDataManager.readDir(path);
        } else {
            return FluentIterable.from(backingFileStore.listDir(path)).transform(
                    new Function<RemoteFileAttributes, RemoteFileAttributes>() {
                        @Override
                        public RemoteFileAttributes apply(final RemoteFileAttributes remoteFileAttributes) {
                            return SqarMetaDataUtil.normalizeSqarFileAttribute(remoteFileAttributes);
                        }
                    }
            ).toList();
        }
    }

    private RemoteFileStore.RemoteFileAttributes getRemoteAttributesImpl(final RemoteCachingPath path) throws IOException {
        final RemoteFileMetadata md = getSqarMetadata(path);
        if (md == null) {
            throw new NoSuchFileException("Could not find metadata for " + path);
        }
        return new RemoteFileStore.RemoteFileAttributes(path, md.getSize(), md.isFile());
    }

    @Override
    RemoteFileStore.RemoteFileAttributes getRemoteAttributes(final RemoteCachingPath path) throws IOException {
        if (isInSqarDirectory(path)) {
            return getRemoteAttributesImpl(path);
        } else {
            return backingFileStore.getRemoteAttributes(path);
        }
    }

    private void downloadFileImpl(final RemoteCachingPath srcPath, final Path destPath) throws IOException {
        final RemoteFileMetadata remoteFileMetadata = getSqarMetadata(srcPath);
        if (remoteFileMetadata == null) {
            throw new NoSuchFileException("Cannot find file for " + srcPath);
        }
        if (!remoteFileMetadata.isFile()) {
            throw new NoSuchFileException(srcPath.toString() + " is not a file");
        }

        final FileMetadata fileMetadata = remoteFileMetadata.getFileMetadata();
        final RemoteCachingPath archivePath = SqarMetaDataUtil.getFullArchivePath(srcPath, fileMetadata.getArchiveFilename());
        try (Closeable ignore = TaskScheduler.RemoteFSIOScheduler.lockSlot()) {
            try (final InputStream archiveIS = backingFileStore.newInputStream(archivePath,
                    fileMetadata.getStartOffset(),
                    remoteFileMetadata.getCompressedSize())) {
                try {
                    sqarMetaDataManager.copyDecompressed(archiveIS, srcPath, destPath, fileMetadata);
                } catch (final IOException e) {
                    Files.delete(destPath);
                    throw Throwables.propagate(e);
                }
            }
        }
    }

    /**
     * true if the contents is within a 'sqar' directory
     */
    boolean isInSqarDirectory(final RemoteCachingPath path) throws IOException {
        final RemoteCachingPath shardPath = SqarMetaDataUtil.getShardPath(path);
        if (shardPath == null) {
            return false;
        }

        final RemoteFileMetadata metadata = getSqarMetadata(shardPath);
        return (metadata != null) && !metadata.isFile();
    }

    @Override
    void downloadFile(final RemoteCachingPath srcPath, final Path destPath) throws IOException {
        if (isInSqarDirectory(srcPath)) {
            downloadFileImpl(srcPath, destPath);
        } else {
            backingFileStore.downloadFile(srcPath, destPath);
        }
    }

    @Nullable
    private RemoteFileMetadata getSqarMetadata(final RemoteCachingPath path) throws IOException {
        return sqarMetaDataManager.getFileMetadata(backingFileStore, path);
    }
}

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

package com.indeed.imhotep.fs.sql;

import com.google.common.base.Function;
import com.google.common.collect.FluentIterable;
import com.indeed.imhotep.archive.FileMetadata;
import com.indeed.imhotep.archive.compression.SquallArchiveCompressor;
import com.indeed.imhotep.fs.RemoteFileListing;
import com.indeed.imhotep.fs.RemoteFileMetadata;
import com.indeed.imhotep.fs.db.metadata.Tables;
import com.indeed.imhotep.fs.db.metadata.tables.Tblfilemetadata;
import com.indeed.imhotep.fs.db.metadata.tables.records.TblfilemetadataRecord;
import com.zaxxer.hikari.HikariDataSource;
import org.apache.commons.io.FilenameUtils;
import org.jooq.BatchBindStep;
import org.jooq.Cursor;
import org.jooq.DSLContext;
import org.jooq.SelectConditionStep;
import org.jooq.impl.DSL;

import java.io.IOException;
import java.nio.file.Path;

/**
 * @author kenh
 */

public class FileMetadataDao implements SqarMetaDataDao {
    private static final String DELIMITER = "/";
    private static final String CONTAINS_DELIMITER_PAT = "%/%";
    private static final Tblfilemetadata TABLE = Tables.TBLFILEMETADATA;
    private final DSLContextContainer dslContextContainer;

    public FileMetadataDao(final HikariDataSource dataSource) {
        dslContextContainer = new DSLContextContainer(dataSource);
    }

    @Override
    public void cacheMetadata(final Path shardPath, final Iterable<RemoteFileMetadata> metadataList) {
        final DSLContext dslContext = dslContextContainer.getDSLContext();

        BatchBindStep batch = dslContext.batch(
                dslContext.mergeInto(TABLE,
                        TABLE.SHARD_NAME,
                        TABLE.FILE_PATH,
                        TABLE.SIZE,
                        TABLE.TIMESTAMP,
                        TABLE.CHECKSUM,
                        TABLE.ARCHIVE_OFFSET,
                        TABLE.COMPRESSOR_TYPE,
                        TABLE.ARCHIVE_NAME,
                        TABLE.IS_FILE,
                        TABLE.PACKED_SIZE
                )
                        .key(TABLE.SHARD_NAME, TABLE.FILE_PATH)
                        .values((String) null, null, null, null, null, null, null, null, null, null)
        );

        for (final RemoteFileMetadata remoteFileMetadata : metadataList) {
            final FileMetadata fileMetadata = remoteFileMetadata.getFileMetadata();
            batch = batch.bind(
                    shardPath.normalize().toString(),
                    remoteFileMetadata.isFile() ? fileMetadata.getFilename() : toNormalizedDirName(fileMetadata.getFilename()),
                    fileMetadata.getSize(),
                    fileMetadata.getTimestamp(),
                    fileMetadata.getChecksum(),
                    fileMetadata.getStartOffset(),
                    fileMetadata.getCompressor().getKey(),
                    fileMetadata.getArchiveFilename(),
                    remoteFileMetadata.isFile(),
                    remoteFileMetadata.getCompressedSize()
            );
        }

        if (batch.size() > 0) {
            batch.execute();
        }
    }

    public static RemoteFileMetadata toRemoteFileMetadata(final TblfilemetadataRecord fetchedRecord) {
        if (fetchedRecord.getIsFile()) {
            final FileMetadata fileMetadata = new FileMetadata(
                    fetchedRecord.getFilePath(),
                    fetchedRecord.getSize(),
                    fetchedRecord.getTimestamp(),
                    fetchedRecord.getChecksum(),
                    fetchedRecord.getArchiveOffset(),
                    SquallArchiveCompressor.fromKey(fetchedRecord.getCompressorType()),
                    fetchedRecord.getArchiveName()
            );
            return new RemoteFileMetadata(fileMetadata, fetchedRecord.getPackedSize());
        } else {
            return new RemoteFileMetadata(
                    FilenameUtils.normalizeNoEndSeparator(fetchedRecord.getFilePath())
            );
        }
    }

    public Cursor<TblfilemetadataRecord> getAllRecords() {
        final DSLContext dslContext = dslContextContainer.getDSLContext();
        return dslContext.selectFrom(TABLE).orderBy(TABLE.SHARD_NAME).fetchLazy();
    }

    public int getNumberOfRecords() {
        final DSLContext dslContext = dslContextContainer.getDSLContext();
        return (Integer)dslContext.selectCount().from(TABLE).fetch().getValue(0, "count");
    }

    @Override
    public RemoteFileMetadata getFileMetadata(final Path shardPath, final String filename) {
        final DSLContext dslContext = dslContextContainer.getDSLContext();

        final TblfilemetadataRecord fetchedRecord = dslContext.selectFrom(TABLE).where(
                TABLE.SHARD_NAME.eq(shardPath.normalize().toString())
        )
                .and(TABLE.FILE_PATH.eq(filename)).fetchAny();

        if (fetchedRecord != null) {
            return toRemoteFileMetadata(fetchedRecord);
        } else {
            return null;
        }
    }

    private static String toNormalizedDirName(final String dirName) {
        if ((dirName.length() > DELIMITER.length()) && dirName.endsWith(DELIMITER)) {
            return dirName.substring(0, dirName.length() - DELIMITER.length());
        }
        return dirName;
    }

    private static String toNormalizedDirNameWithSep(final String dirName) {
        if (dirName.isEmpty() || dirName.endsWith(DELIMITER)) {
            return dirName;
        }
        return dirName + DELIMITER;
    }

    @Override
    public boolean hasShard(final Path shardPath) {
        final DSLContext dslContext = dslContextContainer.getDSLContext();
        return dslContext.select(DSL.count())
                .from(TABLE)
                .where(
                        TABLE.SHARD_NAME.eq(shardPath.normalize().toString())
                ).fetchAny().value1() > 0;
    }

    @Override
    public Iterable<RemoteFileListing> listDirectory(final Path shardPath, final String dirname) {
        final String normalizedDirNameWithSep = toNormalizedDirNameWithSep(dirname);

        final DSLContext dslContext = dslContextContainer.getDSLContext();
        SelectConditionStep<TblfilemetadataRecord> query = dslContext.selectFrom(TABLE).where(
                TABLE.SHARD_NAME.eq(shardPath.normalize().toString())
        )
                .and(TABLE.FILE_PATH.startsWith(normalizedDirNameWithSep))
                .and(TABLE.FILE_PATH.substring(normalizedDirNameWithSep.length() + 1).notLike(CONTAINS_DELIMITER_PAT));

        if (normalizedDirNameWithSep.isEmpty()) {
            query = query.and(TABLE.FILE_PATH.length().gt(0));
        }

        return FluentIterable.from(query.fetchLazy())
                .transform(new Function<TblfilemetadataRecord, RemoteFileListing>() {
                    @Override
                    public RemoteFileListing apply(final TblfilemetadataRecord fetchRecord) {
                        RemoteFileMetadata metadata = toRemoteFileMetadata(fetchRecord);
                        return metadata.isFile() ? new RemoteFileListing(metadata.getFileMetadata()) : new RemoteFileListing(metadata.getFilename());
                    }
                });
    }

    @Override
    public void close() throws IOException {

    }
}

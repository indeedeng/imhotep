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
import com.indeed.imhotep.archive.FileMetadata;
import com.indeed.imhotep.scheduling.TaskScheduler;
import com.indeed.util.core.io.Closeables2;
import org.apache.log4j.Logger;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.NotDirectoryException;
import java.nio.file.Path;
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
                               final Map<String, ?> configuration) throws IOException {
        this.backingFileStore = backingFileStore;
        final File lsmTreeMetadataStore = new File((String)configuration.get("imhotep.fs.sqar.metadata.cache.path"));
        final String lsmTreeExpirationDurationString = (String)(configuration.get("imhotep.fs.sqar.metadata.cache.expiration.hours"));
        final int lsmTreeExpirationDurationHours = lsmTreeExpirationDurationString != null ? Integer.valueOf(lsmTreeExpirationDurationString) : 0;
        final Duration lsmTreeExpirationDuration = lsmTreeExpirationDurationHours > 0 ? Duration.of(lsmTreeExpirationDurationHours, ChronoUnit.HOURS) : null;

        sqarMetaDataDao = new SqarMetaDataLSMStore(lsmTreeMetadataStore, lsmTreeExpirationDuration);
        sqarMetaDataManager = new SqarMetaDataManager(sqarMetaDataDao);
    }

    @Override
    public void close() {
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
        try (final Closeable ignore = TaskScheduler.RemoteFSIOScheduler.lockSlot()) {
            try (final InputStream archiveIS = backingFileStore.newInputStream(archivePath,
                    fileMetadata.getStartOffset(),
                    remoteFileMetadata.getCompressedSize())) {
                try {
                    sqarMetaDataManager.copyDecompressed(archiveIS, srcPath, destPath, fileMetadata);
                } catch (final IOException e) {
                    try {
                        Files.delete(destPath);
                    } catch (final Throwable throwable) {
                        e.addSuppressed(throwable);
                    }
                    throw e;
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

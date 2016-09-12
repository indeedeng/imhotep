package com.indeed.imhotep.fs;

import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.google.common.collect.FluentIterable;
import com.indeed.imhotep.archive.FileMetadata;
import com.indeed.imhotep.fs.db.metadata.Tables;
import com.indeed.imhotep.fs.sql.FileMetadataDao;
import com.indeed.imhotep.fs.sql.SchemaInitializer;
import com.indeed.imhotep.fs.sql.SqarMetaDataDao;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.NotDirectoryException;
import java.nio.file.Path;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * @author kenh
 */
class SqarRemoteFileStore extends RemoteFileStore implements Closeable {
    private final SqarMetaDataManager sqarMetaDataManager;
    private final HikariDataSource dataSource;
    private final SqarMetaDataDao sqarMetaDataDao;
    private final RemoteFileStore backingFileStore;

    SqarRemoteFileStore(final RemoteFileStore backingFileStore,
                               final Map<String, ?> configuration) throws SQLException, ClassNotFoundException, IOException {
        this.backingFileStore = backingFileStore;

        final File dbFile = new File((String) configuration.get("imhotep.fs.sqardb.file"));
        final HikariConfig config = new HikariConfig();
        config.setJdbcUrl("jdbc:h2:" + dbFile);


        dataSource = new HikariDataSource(config);
        new SchemaInitializer(dataSource).initialize(Collections.singletonList(Tables.TBLFILEMETADATA));

        sqarMetaDataDao = new FileMetadataDao(dataSource);
        sqarMetaDataManager = new SqarMetaDataManager(sqarMetaDataDao);
    }

    @Override
    public void close() throws IOException {
        dataSource.close();
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
        if (SqarMetaDataUtil.isInSqarDirectory(backingFileStore, path)) {
            final RemoteFileMetadata sqarMetadata = getSqarMetadata(path);
            if (sqarMetadata == null) {
                throw new NoSuchFileException(path.toString());
            } else if (sqarMetadata.isFile()) {
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
        if (SqarMetaDataUtil.isInSqarDirectory(backingFileStore, path)) {
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

    @Override
    void downloadFile(final RemoteCachingPath srcPath, final Path destPath) throws IOException {
        if (SqarMetaDataUtil.isInSqarDirectory(backingFileStore, srcPath)) {
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

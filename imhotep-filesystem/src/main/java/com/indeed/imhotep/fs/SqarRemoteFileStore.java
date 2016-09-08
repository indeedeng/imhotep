package com.indeed.imhotep.fs;

import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.google.common.collect.FluentIterable;
import com.indeed.imhotep.archive.FileMetadata;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.NotDirectoryException;
import java.nio.file.Path;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

/**
 * @author darren
 */
class SqarRemoteFileStore extends RemoteFileStore implements Closeable {
    private final SqarMetaDataManager sqarMetaDataManager;
    private final RemoteFileStore backingFileStore;

    SqarRemoteFileStore(final RemoteFileStore backingFileStore,
                               final Map<String, ?> configuration) throws SQLException, ClassNotFoundException {
        this.backingFileStore = backingFileStore;
        sqarMetaDataManager = new SqarMetaDataManager(configuration);
    }

    @Override
    public void close() throws IOException {
        sqarMetaDataManager.close();
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
        if (SqarMetaDataManager.isInSqarDirectory(path, backingFileStore)) {
            final FileMetadata sqarMetadata = getSqarMetadata(path);
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
                            return SqarMetaDataManager.normalizeSqarFileAttribute(remoteFileAttributes);
                        }
                    }
            ).toList();
        }
    }

    private RemoteFileStore.RemoteFileAttributes getRemoteAttributesImpl(final RemoteCachingPath path) throws IOException {
        final FileMetadata md = getSqarMetadata(path);
        if (md == null) {
            throw new NoSuchFileException("Could not find metadata for " + path);
        }
        return new RemoteFileStore.RemoteFileAttributes(path, md.getSize(), md.isFile());
    }

    @Override
    RemoteFileStore.RemoteFileAttributes getRemoteAttributes(final RemoteCachingPath path) throws IOException {
        if (SqarMetaDataManager.isInSqarDirectory(path, backingFileStore)) {
            return getRemoteAttributesImpl(path);
        } else {
            return backingFileStore.getRemoteAttributes(path);
        }
    }

    private void downloadFileImpl(final RemoteCachingPath srcPath, final Path destPath) throws IOException {
        final FileMetadata md = getSqarMetadata(srcPath);
        if (md == null) {
            throw new NoSuchFileException("Cannot find file for " + srcPath);
        }
        if (!md.isFile()) {
            throw new NoSuchFileException(srcPath.toString() + " is not a file");
        }

        final RemoteCachingPath archivePath = SqarMetaDataManager.getFullArchivePath(srcPath, md.getArchiveFilename());
        try (final InputStream archiveIS = backingFileStore.newInputStream(archivePath,
                md.getStartOffset(),
                md.getCompressedSize())) {
            try {
                sqarMetaDataManager.copyDecompressed(archiveIS, srcPath, destPath, md);
            } catch (final IOException e) {
                Files.delete(destPath);
                throw Throwables.propagate(e);
            }
        }
    }

    @Override
    void downloadFile(final RemoteCachingPath srcPath, final Path destPath) throws IOException {
        if (SqarMetaDataManager.isInSqarDirectory(srcPath, backingFileStore)) {
            downloadFileImpl(srcPath, destPath);
        } else {
            backingFileStore.downloadFile(srcPath, destPath);
        }
    }

    @Nullable
    private FileMetadata getSqarMetadata(final RemoteCachingPath path) throws IOException {
        SqarMetaDataManager.PathInfoResult pathInfoResult = sqarMetaDataManager.getPathInfo(path);

        if (pathInfoResult == SqarMetaDataManager.PathInfoResult.ARCHIVE_MISSING) {
            final RemoteCachingPath metadataPath = SqarMetaDataManager.getMetadataPath(path);
            try (InputStream metadataIS = backingFileStore.newInputStream(metadataPath, 0, -1)) {
                sqarMetaDataManager.cacheMetadata(path, metadataIS);
            }
            pathInfoResult = sqarMetaDataManager.getPathInfo(path);
        }
        if (pathInfoResult == SqarMetaDataManager.PathInfoResult.FILE_MISSING) {
            return null;
        }

        return pathInfoResult.metadata;
    }
}

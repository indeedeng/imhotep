package com.indeed.imhotep.fs;

import com.google.common.base.Function;
import com.google.common.collect.FluentIterable;
import com.indeed.imhotep.archive.ArchiveUtils;
import com.indeed.imhotep.archive.FileMetadata;
import com.indeed.imhotep.archive.SquallArchiveReader;
import com.indeed.imhotep.fs.sql.SqarMetaDataDao;

import javax.annotation.Nullable;
import java.io.BufferedOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.DigestInputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * @author kenh
 */

class SqarMetaDataManager {
    private final SqarMetaDataDao sqarMetaDataDao;

    SqarMetaDataManager(final SqarMetaDataDao sqarMetaDataDao) {
        this.sqarMetaDataDao = sqarMetaDataDao;
    }

    private static List<RemoteFileMetadata> readMetadata(final InputStream metadataIS) throws IOException {
        final List<FileMetadata> fileMetadataList = SquallArchiveReader.readMetadata(metadataIS);
        final List<RemoteFileMetadata> remoteFileMetadataList = new ArrayList<>();

        // sorted by archive and file position
        Collections.sort(fileMetadataList, new Comparator<FileMetadata>() {
            @Override
            public int compare(final FileMetadata metadata1, final FileMetadata metadata2) {
                final String fmd1Archive = metadata1.getArchiveFilename();
                final String fmd2Archive = metadata2.getArchiveFilename();

                final int cmpResult = fmd1Archive.compareTo(fmd2Archive);
                if (cmpResult != 0) {
                    return cmpResult;
                }

                final long fmd1Offset = metadata1.getStartOffset();
                final long fmd2Offset = metadata2.getStartOffset();
                return Long.compare(fmd1Offset, fmd2Offset);
            }
        });

        for (int i = 0; i < fileMetadataList.size(); i++) {
            final FileMetadata fileMetadata = fileMetadataList.get(i);
            final long compressedSize;
            if (i == (fileMetadataList.size() - 1)) {
                compressedSize  = -1; // until the end
            } else {
                compressedSize = fileMetadataList.get(i + 1).getStartOffset() - fileMetadata.getStartOffset();
            }
            remoteFileMetadataList.add(new RemoteFileMetadata(fileMetadata, compressedSize));
        }

        return remoteFileMetadataList;
    }

    private void cacheMetadata(final RemoteCachingPath shardPath, final InputStream metadataIS) throws IOException {
        final List<RemoteFileMetadata> fileList = readMetadata(metadataIS);
        final Set<String> dirList = new HashSet<>();

        for (final RemoteFileMetadata remoteMetadata : fileList) {
            final String fname = remoteMetadata.getFilename();
            Path path = Paths.get(fname);
            while (true) {
                path = path.getParent();
                if (path != null) {
                    dirList.add(path.toString());
                } else {
                    dirList.add("");
                    break;
                }
            }
        }
        for (final String dir : dirList) {
            fileList.add(new RemoteFileMetadata(dir));
        }

        sqarMetaDataDao.cacheMetadata(shardPath, fileList);
    }

    /**
     * get the metadata corresponding to the path
     * @param fs the file store implementation
     * @param path the path for which to fetch the metadata
     * @return the metadata. null if no file corresponding to {@param path}
     */
    @Nullable
    RemoteFileMetadata getFileMetadata(final RemoteFileStore fs, final RemoteCachingPath path) throws IOException {
        final RemoteCachingPath shardPath = SqarMetaDataUtil.getShardPath(path);
        final String fileName = SqarMetaDataUtil.getFilePath(path).toString();
        final RemoteFileMetadata fileMetadata = sqarMetaDataDao.getFileMetadata(shardPath, fileName);
        if (fileMetadata == null) {
            if (!sqarMetaDataDao.hasShard(shardPath)) {
                try (InputStream metadataInputStream = fs.newInputStream(SqarMetaDataUtil.getMetadataPath(path), 0, -1)) {
                    cacheMetadata(shardPath, metadataInputStream);
                } catch (final NoSuchFileException|FileNotFoundException e) {
                    // when the metadata file doesn't exist, there is nothing to return
                    return null;
                }
                return sqarMetaDataDao.getFileMetadata(shardPath, fileName);
            }
        }
        return fileMetadata;
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

    List<RemoteFileStore.RemoteFileAttributes> readDir(final RemoteCachingPath path) {
        final RemoteCachingPath shardPath = SqarMetaDataUtil.getShardPath(path);
        final String fileName = SqarMetaDataUtil.getFilePath(path).toString();
        if ((shardPath == null) && (fileName == null)) {
            return Collections.emptyList();
        }
        return FluentIterable.from(sqarMetaDataDao.listDirectory(shardPath, fileName)).transform(new Function<RemoteFileMetadata, RemoteFileStore.RemoteFileAttributes>() {
            @Override
            public RemoteFileStore.RemoteFileAttributes apply(final RemoteFileMetadata fileMetadata) {
                return new RemoteFileStore.RemoteFileAttributes(shardPath.resolve(fileMetadata.getFilename()), fileMetadata.getSize(), fileMetadata.isFile());
            }
        }).toList();
    }
}

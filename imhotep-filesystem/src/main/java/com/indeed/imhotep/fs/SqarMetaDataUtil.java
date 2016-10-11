package com.indeed.imhotep.fs;

import javax.annotation.Nullable;

/**
 * @author kenh
 *
 * On the {@link RemoteCachingFileSystem} we need special handling for files in sqar archives
 * and we need to be able to do file operations on files within the archive without fully expanding them
 * Sqar archives always follow a format of "/dataset/shard.sqar".
 * If we try to access a file like "/dataset/shard/path/to/file",
 * we need to identify the shard directory "/dataset/shard.sqar" and check within the archive file
 * "/dataset/shard.sqar/archiveN.bin" for "path/to/file"
 */

class SqarMetaDataUtil {
    private static final String SQAR_SUFFIX = ".sqar";
    private static final String METADATA_FILE = "metadata.txt";
    private static final int SHARD_PATH_COMPONENT = 2;

    private SqarMetaDataUtil() {
    }

    @Nullable
    static RemoteCachingPath getShardPath(final RemoteCachingPath path) {
        if (path.getNameCount() >= SHARD_PATH_COMPONENT) {
            final RemoteCachingPath relShardPath = (RemoteCachingPath) path.subpath(0, SHARD_PATH_COMPONENT);
            if (path.isAbsolute()) {
                return (RemoteCachingPath) RemoteCachingPath.getRoot(path.getFileSystem()).resolve(relShardPath);
            } else {
                return relShardPath;
            }
        }
        return null;
    }

    @Nullable
    static RemoteCachingPath getFilePath(final RemoteCachingPath path) {
         if (path.getNameCount() >= (SHARD_PATH_COMPONENT + 1)) {
            return (RemoteCachingPath) path.subpath(SHARD_PATH_COMPONENT, path.getNameCount());
        } else if (path.getNameCount() == SHARD_PATH_COMPONENT) {
            return (RemoteCachingPath) path.getFileSystem().getPath("");
        }
        return null;
    }

    static RemoteCachingPath getSqarPath(final RemoteCachingPath path) {
        final RemoteCachingPath shardPath = getShardPath(path);
        if (shardPath != null) {
            final RemoteCachingPath sqarPath;
            if (!shardPath.getFileName().toString().endsWith(SQAR_SUFFIX)) {
                sqarPath = ((RemoteCachingPath) shardPath.getParent()).resolve(shardPath.getFileName() + SQAR_SUFFIX);
            } else {
                sqarPath = shardPath;
            }

            return sqarPath;
        }
        return null;
    }

    static RemoteCachingPath getMetadataPath(final RemoteCachingPath path) {
        final RemoteCachingPath sqarPath = getSqarPath(path);
        if (sqarPath != null) {
            return sqarPath.resolve(METADATA_FILE);
        }
        return null;
    }

    static RemoteCachingPath getFullArchivePath(final RemoteCachingPath path, final String archiveFile) {
        final RemoteCachingPath sqarPath = getSqarPath(path);
        if (sqarPath != null) {
            return sqarPath.resolve(archiveFile);
        }
        return null;
    }

    private static boolean isSqarDirectory(final RemoteFileStore.RemoteFileAttributes fileAttributes) {
        if (fileAttributes.isDirectory()) {
            if (fileAttributes.getPath().getNameCount() == SHARD_PATH_COMPONENT) {
                return (fileAttributes.getPath().getFileName().toString().endsWith(SQAR_SUFFIX));
            }
        }
        return false;
    }

    static RemoteFileStore.RemoteFileAttributes normalizeSqarFileAttribute(final RemoteFileStore.RemoteFileAttributes fileAttributes) {
        if (isSqarDirectory(fileAttributes)) {
            final String pathStr = fileAttributes.getPath().toString();
            return new RemoteFileStore.RemoteFileAttributes(
                    new RemoteCachingPath(fileAttributes.getPath().getFileSystem(), pathStr.substring(0, pathStr.length() - SQAR_SUFFIX.length())),
                    fileAttributes.getSize(),
                    fileAttributes.isFile()
            );
        } else {
            return fileAttributes;
        }
    }

}

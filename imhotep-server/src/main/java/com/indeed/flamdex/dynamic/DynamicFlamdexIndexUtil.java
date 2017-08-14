package com.indeed.flamdex.dynamic;

import com.google.common.base.Optional;
import com.indeed.flamdex.dynamic.locks.MultiThreadFileLockUtil;
import com.indeed.flamdex.dynamic.locks.MultiThreadLock;
import com.indeed.util.core.shell.PosixFileOperations;
import com.indeed.util.io.Directories;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * @author michihiko
 */

public final class DynamicFlamdexIndexUtil {
    // {dataset}/{indexDirectory}/reader.lock : Taken while there is a reader reads from the index.
    private static final String READER_LOCK_FILENAME = "reader.lock";
    // {dataset}/{indexDirectoryPrefix}.writer.lock : Taken while there is a writer writes into the index.
    private static final String WRITER_LOCK_FILENAME_FORMAT = "%s.writer.lock";

    private DynamicFlamdexIndexUtil() {
    }

    @Nonnull
    public static MultiThreadLock acquireReaderLock(@Nonnull final Path indexDirectory) throws IOException {
        final Optional<MultiThreadLock> readLockOrAbsent = MultiThreadFileLockUtil.tryReadLock(indexDirectory, READER_LOCK_FILENAME);
        if (!readLockOrAbsent.isPresent()) {
            throw new IOException("Failed to open index " + indexDirectory);
        }
        return readLockOrAbsent.get();
    }

    @Nonnull
    static MultiThreadLock acquireWriterLock(@Nonnull final Path datasetDirectory, @Nonnull final String indexDirectoryPrefix) throws IOException {
        return MultiThreadFileLockUtil.writeLock(datasetDirectory, String.format(WRITER_LOCK_FILENAME_FORMAT, indexDirectoryPrefix));
    }

    @Nonnull
    static List<Path> listSegmentDirectories(@Nonnull final Path indexDirectory) throws IOException {
        final List<Path> segmentDirectories = new ArrayList<>();
        for (final Path segmentDirectory : Directories.list(indexDirectory)) {
            if (Files.isDirectory(segmentDirectory)) {
                segmentDirectories.add(segmentDirectory);
            }
        }
        Collections.sort(segmentDirectories);
        return segmentDirectories;
    }

    static void removeDirectoryRecursively(@Nonnull final Path path) throws IOException {
        PosixFileOperations.rmrf(path);
    }

    static void createHardLinksRecursively(@Nonnull final Path source, @Nonnull final Path dest) throws IOException {
        PosixFileOperations.cplr(source, dest);
    }

    public static boolean tryRemoveIndex(@Nonnull final Path indexDirectory) throws IOException {
        final Optional<MultiThreadLock> lockOrEmpty = MultiThreadFileLockUtil.tryWriteLock(indexDirectory, READER_LOCK_FILENAME);
        if (lockOrEmpty.isPresent()) {
            try {
                final Path tempDirectoryName = indexDirectory.getParent().resolve("deleting." + indexDirectory.getFileName().toString());
                while (true) {
                    try {
                        Files.move(indexDirectory, tempDirectoryName);
                        removeDirectoryRecursively(tempDirectoryName);
                        return true;
                    } catch (final FileAlreadyExistsException e) {
                        // In the case we already have that temp directory, remove that first and retry.
                        removeDirectoryRecursively(tempDirectoryName);
                    }
                }
            } finally {
                lockOrEmpty.get().close();
            }
        } else {
            return false;
        }
    }
}

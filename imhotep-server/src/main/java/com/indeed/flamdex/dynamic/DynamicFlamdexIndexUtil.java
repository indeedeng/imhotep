package com.indeed.flamdex.dynamic;

import com.google.common.base.Optional;
import com.indeed.flamdex.dynamic.locks.MultiThreadFileLockUtil;
import com.indeed.flamdex.dynamic.locks.MultiThreadLock;
import com.indeed.util.io.Directories;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * @author michihiko
 */

final class DynamicFlamdexIndexUtil {
    // {dataset}/{indexDirectory}/reader.lock : Taken while there is a reader reads from the index.
    private static final String READER_LOCK_FILENAME = "reader.lock";
    // {dataset}/{indexDirectoryPrefix}.writer.lock : Taken while there is a writer writes into the index.
    private static final String WRITER_LOCK_FILENAME_FORMAT = "%s.writer.lock";

    private DynamicFlamdexIndexUtil() {
    }

    @Nonnull
    static MultiThreadLock acquireReaderLock(@Nonnull final Path indexDirectory) throws IOException {
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
}

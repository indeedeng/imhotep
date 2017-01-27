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

final class DynamicFlamdexShardUtil {
    // {dataset}/{shardDirectory}/reader.lock : Taken while there is a reader reads from the shard.
    private static final String READER_LOCK_FILENAME = "reader.lock";
    // {dataset}/{shardName}.writer.lock : Taken while there is a writer writes into the shard.
    private static final String WRITER_LOCK_FILENAME_FORMAT = "%s.writer.lock";

    private DynamicFlamdexShardUtil() {
    }

    @Nonnull
    static MultiThreadLock acquireReaderLock(@Nonnull final Path shardDirectory) throws IOException {
        final Optional<MultiThreadLock> readLockOrAbsent = MultiThreadFileLockUtil.tryReadLock(shardDirectory, READER_LOCK_FILENAME);
        if (!readLockOrAbsent.isPresent()) {
            throw new IOException("Failed to open " + shardDirectory);
        }
        return readLockOrAbsent.get();
    }

    @Nonnull
    static MultiThreadLock acquireWriterLock(@Nonnull final Path datasetDirectory, @Nonnull final String shardDirectoryPrefix) throws IOException {
        return MultiThreadFileLockUtil.writeLock(datasetDirectory, String.format(WRITER_LOCK_FILENAME_FORMAT, shardDirectoryPrefix));
    }

    @Nonnull
    static List<Path> listSegmentDirectories(@Nonnull final Path shardDirectory) throws IOException {
        final List<Path> segmentDirectories = new ArrayList<>();
        for (final Path segmentDirectory : Directories.list(shardDirectory)) {
            if (Files.isDirectory(segmentDirectory)) {
                segmentDirectories.add(segmentDirectory);
            }
        }
        Collections.sort(segmentDirectories);
        return segmentDirectories;
    }
}

package com.indeed.flamdex.dynamic;

import com.google.common.base.Optional;
import com.indeed.flamdex.dynamic.locks.MultiThreadFileLockUtil;
import com.indeed.flamdex.dynamic.locks.MultiThreadLock;
import com.indeed.util.io.Directories;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
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
        Files.walkFileTree(path, new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult visitFile(@Nonnull final Path file, @Nonnull final BasicFileAttributes attributes) throws IOException {
                Files.delete(file);
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult postVisitDirectory(@Nonnull final Path dir, @Nullable final IOException exception) throws IOException {
                if (exception != null) {
                    throw exception;
                }
                Files.delete(dir);
                return FileVisitResult.CONTINUE;
            }
        });
    }

    static void createHardLinksRecursively(@Nonnull final Path source, @Nonnull final Path dest) throws IOException {
        Files.createDirectories(dest.getParent());
        Files.walkFileTree(source, new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult preVisitDirectory(@Nonnull final Path dir, @Nonnull final BasicFileAttributes attrs) throws IOException {
                Files.createDirectories(dest.resolve(source.relativize(dir)));
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult visitFile(@Nonnull final Path file, @Nonnull final BasicFileAttributes attributes) throws IOException {
                final Path destFilePath = dest.resolve(source.relativize(file));
                Files.createLink(destFilePath, file);
                return FileVisitResult.CONTINUE;
            }
        });
    }

    public static void removeIndex(@Nonnull final Path indexDirectory) throws IOException {
        final Path tempDirectory = Files.createTempDirectory(indexDirectory.getParent(), "deleting." + indexDirectory.getFileName().toString());
        try (final MultiThreadLock lock = MultiThreadFileLockUtil.writeLock(indexDirectory, READER_LOCK_FILENAME)) {
            Files.move(indexDirectory, tempDirectory.resolve(indexDirectory.getFileName()));
        } finally {
            removeDirectoryRecursively(tempDirectory);
        }
    }
}

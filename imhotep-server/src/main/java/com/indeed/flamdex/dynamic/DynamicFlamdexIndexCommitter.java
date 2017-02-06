package com.indeed.flamdex.dynamic;

import com.google.common.collect.ImmutableList;
import com.indeed.flamdex.datastruct.FastBitSet;
import com.indeed.flamdex.reader.FlamdexMetadata;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.Closeable;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author michihiko
 */

class DynamicFlamdexIndexCommitter implements Closeable {

    private Long latestVersion;
    private final Path temporaryDirectory;
    private final Path datasetDirectory;
    private final String indexDirectoryPrefix;
    private final Path temporaryIndexDirectory;
    private final Set<Path> currentSegments;
    private final ReentrantLock changeSegmentsLock = new ReentrantLock(true);
    private String lastGeneratedTimestamp = null;

    DynamicFlamdexIndexCommitter(@Nonnull final Path datasetDirectory, @Nonnull final String indexDirectoryPrefix, @Nullable final Path latestIndexDirectory) throws IOException {
        latestVersion = null;
        this.datasetDirectory = datasetDirectory;
        this.indexDirectoryPrefix = indexDirectoryPrefix;
        temporaryDirectory = Files.createTempDirectory(datasetDirectory.getFileName().toString());
        temporaryIndexDirectory = temporaryDirectory.resolve("index");
        Files.createDirectory(temporaryIndexDirectory);
        currentSegments = new HashSet<>();
        if (latestIndexDirectory != null) {
            final FlamdexMetadata metadata = FlamdexMetadata.readMetadata(latestIndexDirectory);
            if (metadata.formatVersion == DynamicFlamdexDocWriter.FORMAT_VERSION) {
                // Write on top of dynamic index
                for (final Path segmentDirectory : DynamicFlamdexIndexUtil.listSegmentDirectories(latestIndexDirectory)) {
                    final Path copiedSegmentPath = temporaryIndexDirectory.resolve(segmentDirectory.getFileName());
                    createHardLinksRecursively(segmentDirectory, copiedSegmentPath);
                    currentSegments.add(copiedSegmentPath);
                }
            } else {
                // Write on top of other type of index
                final Path copiedSegmentPath = newSegmentDirectory();
                createHardLinksRecursively(latestIndexDirectory, copiedSegmentPath);
                currentSegments.add(copiedSegmentPath);
            }
        }
    }

    private static void createHardLinksRecursively(@Nonnull final Path source, @Nonnull final Path dest) throws IOException {
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

    private static void removeDirectoryRecursively(@Nonnull final Path path) throws IOException {
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

    @Nonnull
    private synchronized String generateTimestamp() {
        String timestamp;
        do {
            timestamp = String.valueOf(System.currentTimeMillis());
        } while (timestamp.equals(lastGeneratedTimestamp));
        lastGeneratedTimestamp = timestamp;
        return timestamp;
    }

    @Nonnull
    private String generateIndexDirectoryName(@Nonnull final Long version) {
        return indexDirectoryPrefix + '.' + version + '.' + generateTimestamp();
    }

    public void close() throws IOException {
        removeDirectoryRecursively(temporaryDirectory);
    }

    @Nonnull
    public ReentrantLock getChangeSegmentsLock() {
        return changeSegmentsLock;
    }

    @Nonnull
    private String generateSegmentName() {
        return "segment." + generateTimestamp();
    }

    @Nonnull
    private FlamdexMetadata generateFlamdexMetadata() throws IOException {
        int numDocs = 0;
        final Set<String> intFields = new TreeSet<>();
        final Set<String> stringFields = new TreeSet<>();
        for (final Path segmentDirectory : currentSegments) {
            final FlamdexMetadata segmentMetadata = FlamdexMetadata.readMetadata(segmentDirectory);
            numDocs += segmentMetadata.getNumDocs();
            intFields.addAll(segmentMetadata.getIntFields());
            stringFields.addAll(segmentMetadata.getStringFields());
        }
        return new FlamdexMetadata(numDocs, ImmutableList.copyOf(intFields), ImmutableList.copyOf(stringFields), DynamicFlamdexDocWriter.FORMAT_VERSION);
    }

    @Nonnull
    public Path newSegmentDirectory() throws IOException {
        final String newSegmentName = generateSegmentName();
        final Path newSegmentDirectory = temporaryIndexDirectory.resolve(newSegmentName);
        Files.createDirectories(newSegmentDirectory);
        return newSegmentDirectory;
    }

    @Nonnull
    public Collection<Path> getCurrentSegmentPaths() {
        changeSegmentsLock.lock();
        try {
            return currentSegments;
        } finally {
            changeSegmentsLock.unlock();
        }
    }

    @Nonnull
    public Path commit(@Nonnull final Long version) throws IOException {
        final Path tempCommitDirectory = Files.createTempDirectory(temporaryDirectory, "commit");
        changeSegmentsLock.lock();
        try {
            final Path newIndexDirectory = datasetDirectory.resolve(generateIndexDirectoryName(version));
            for (final Path segmentPath : currentSegments) {
                final Path segmentName = segmentPath.getFileName();
                createHardLinksRecursively(segmentPath, tempCommitDirectory.resolve(segmentName));
            }
            final FlamdexMetadata metadata = generateFlamdexMetadata();
            FlamdexMetadata.writeMetadata(tempCommitDirectory, metadata);
            Files.move(tempCommitDirectory, newIndexDirectory);
            latestVersion = version;
            return newIndexDirectory;
        } catch (final Throwable e) {
            removeDirectoryRecursively(tempCommitDirectory);
            throw e;
        } finally {
            changeSegmentsLock.unlock();
        }
    }

    public void commit() throws IOException {
        if (latestVersion == null) {
            throw new RuntimeException("Please commit with version name if you haven't committed yet");
        }
        commit(latestVersion);
    }

    public void addSegment(@Nonnull final Path newSegmentDirectory, @Nonnull final Map<Path, FastBitSet> updatedTombstoneSets) throws IOException {
        changeSegmentsLock.lock();
        try {
            for (final Map.Entry<Path, FastBitSet> segmentAndTombstoneSet : updatedTombstoneSets.entrySet()) {
                final Path segmentName = segmentAndTombstoneSet.getKey().getFileName();
                DynamicFlamdexSegmentUtil.writeTombstoneSet(temporaryIndexDirectory.resolve(segmentName), segmentAndTombstoneSet.getValue());
            }
            currentSegments.add(newSegmentDirectory);
        } finally {
            changeSegmentsLock.unlock();
        }
    }

    public void replaceSegments(@Nonnull final Collection<Path> removeSegmentDirectories, @Nonnull final Path newSegmentDirectory) {
        changeSegmentsLock.lock();
        try {
            currentSegments.add(newSegmentDirectory);
            currentSegments.removeAll(removeSegmentDirectories);
        } finally {
            changeSegmentsLock.unlock();
        }
    }
}

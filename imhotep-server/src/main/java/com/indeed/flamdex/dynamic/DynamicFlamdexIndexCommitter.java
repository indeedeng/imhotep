package com.indeed.flamdex.dynamic;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.indeed.flamdex.datastruct.FastBitSet;
import com.indeed.flamdex.query.Query;
import com.indeed.flamdex.reader.FlamdexFormatVersion;
import com.indeed.flamdex.reader.FlamdexMetadata;
import com.indeed.util.core.time.DefaultWallClock;
import com.indeed.util.core.time.WallClock;
import org.apache.log4j.Logger;

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
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author michihiko
 */

class DynamicFlamdexIndexCommitter implements Closeable {

    private static final Logger LOG = Logger.getLogger(DynamicFlamdexIndexCommitter.class);

    private Long latestVersion;
    private final Path temporaryDirectory;
    private final Path datasetDirectory;
    private final String indexDirectoryPrefix;
    private final Path temporaryIndexDirectory;
    private final Set<Path> currentSegments;
    private final WallClock wallClock;
    private final ReentrantLock changeSegmentsLock = new ReentrantLock(true);
    private String lastGeneratedTimestamp = null;

    DynamicFlamdexIndexCommitter(
            @Nonnull final Path datasetDirectory,
            @Nonnull final String indexDirectoryPrefix,
            @Nullable final Long latestVersion,
            @Nullable final Path latestIndexDirectory
    ) throws IOException {
        this(datasetDirectory, indexDirectoryPrefix, latestVersion, latestIndexDirectory, new DefaultWallClock());
    }

    DynamicFlamdexIndexCommitter(
            @Nonnull final Path datasetDirectory,
            @Nonnull final String indexDirectoryPrefix,
            @Nullable final Long latestVersion,
            @Nullable final Path latestIndexDirectory,
            @Nonnull final WallClock wallClock
    ) throws IOException {
        this.datasetDirectory = datasetDirectory;
        this.indexDirectoryPrefix = indexDirectoryPrefix;
        this.temporaryDirectory = Files.createTempDirectory(datasetDirectory,
                "TempDir." + indexDirectoryPrefix);
        temporaryIndexDirectory = this.temporaryDirectory.resolve("index");
        Files.createDirectory(temporaryIndexDirectory);
        currentSegments = new HashSet<>();
        this.latestVersion = latestVersion;
        this.wallClock = wallClock;
        if (latestIndexDirectory != null) {
            final FlamdexMetadata metadata = FlamdexMetadata.readMetadata(latestIndexDirectory);
            if (FlamdexFormatVersion.DYNAMIC == metadata.getFlamdexFormatVersion()) {
                // Write on top of dynamic index.
                for (final Path segmentDirectory : DynamicFlamdexIndexUtil.listSegmentDirectories(latestIndexDirectory)) {
                    final Path copiedSegmentPath = temporaryIndexDirectory.resolve(segmentDirectory.getFileName());
                    createHardLinksRecursively(segmentDirectory, copiedSegmentPath);
                    currentSegments.add(copiedSegmentPath);
                }
            } else {
                // Write on top of other type of index, treat it as a segment.
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
            timestamp = String.valueOf(wallClock.currentTimeMillis());
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
        return new FlamdexMetadata(numDocs, ImmutableList.copyOf(intFields), ImmutableList.copyOf(stringFields), FlamdexFormatVersion.DYNAMIC);
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
            return ImmutableList.copyOf(currentSegments);
        } finally {
            changeSegmentsLock.unlock();
        }
    }

    /**
     * Commit current index with specified version.
     *
     * @return The path of the index made by the commit.
     */
    @VisibleForTesting
    @Nonnull
    protected Path commit(@Nonnull final Long version) throws IOException {
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

    /**
     * Commit current version if the version name available
     * (i.e. {@code latestVersion} is given in the constructor, or at least one {@link DynamicFlamdexIndexCommitter#commit} is called.
     * If not, we postpone the commit until next {@link DynamicFlamdexIndexCommitter#commit} call.
     *
     * @return The path of the index made by this commit, or absent if we couldn't commit.
     */
    @Nonnull
    private Optional<Path> commitIfPossible() throws IOException {
        if (latestVersion != null) {
            return Optional.of(commit(latestVersion));
        }
        return Optional.absent();
    }

    /**
     * First, update tombstone set of current segments by applying {@code deleteQuery},
     * and then add {@code newSegmentDirectory} into current segments, and commit with version {@code version}.
     * If this method rises an exception, the consistency inside temporary directory could be broken.
     *
     * @return The path of the index made by this commit.
     */
    @Nonnull
    public Path addSegmentWithDeletionAndCommit(
            final long version,
            @Nonnull final Path newSegmentDirectory,
            @Nullable final Query deleteQuery
    ) throws IOException {
        changeSegmentsLock.lock();
        try {
            if (deleteQuery != null) {
                // Update tombstone set on current segments.
                for (final Path segmentPath : currentSegments) {
                    try (final SegmentReader segmentReader = new SegmentReader(segmentPath)) {
                        final Optional<FastBitSet> updatedTombstoneSet = segmentReader.getUpdatedTombstoneSet(deleteQuery);
                        if (updatedTombstoneSet.isPresent()) {
                            DynamicFlamdexSegmentUtil.writeTombstoneSet(segmentPath, updatedTombstoneSet.get());
                        }
                    }
                }
            }
            currentSegments.add(newSegmentDirectory);
            return commit(version);
        } finally {
            changeSegmentsLock.unlock();
        }
    }

    /**
     * Remove {@code removeSegmentDirectories}, and add {@code newSegmentDirectory} to current segments.
     * After that, we try to commit this change, and if we failed (see: {@link DynamicFlamdexIndexCommitter#commitIfPossible()}),
     * we postpone it until commit with version name has called.
     *
     * @return The path of the index made by this commit, or absent if we couldn't commit.
     */
    @Nonnull
    public Optional<Path> replaceSegmentsAndCommitIfPossible(@Nonnull final Collection<Path> removeSegmentDirectories, @Nonnull final Path newSegmentDirectory) throws IOException {
        changeSegmentsLock.lock();
        try {
            currentSegments.add(newSegmentDirectory);
            currentSegments.removeAll(removeSegmentDirectories);
            for (final Path oldSegmentDirectory : removeSegmentDirectories) {
                try {
                    removeDirectoryRecursively(oldSegmentDirectory);
                } catch (final Throwable e) {
                    LOG.error("Failed to remove directory " + oldSegmentDirectory, e);
                }
            }
            return commitIfPossible();
        } finally {
            changeSegmentsLock.unlock();
        }
    }
}

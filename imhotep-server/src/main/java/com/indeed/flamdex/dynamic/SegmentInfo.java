package com.indeed.flamdex.dynamic;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.indeed.flamdex.datastruct.FastBitSet;
import com.indeed.flamdex.dynamic.locks.MultiThreadFileLockUtil;
import com.indeed.flamdex.dynamic.locks.MultiThreadLock;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;

/**
 * @author michihiko
 */

class SegmentInfo implements Comparable<SegmentInfo> {
    private static final String SEGMENT_LOCK_FILENAME = "reader.lock";

    private final Path shardDirectory;
    private final String segmentName;
    private final Optional<String> tombstoneFileName;

    SegmentInfo(@Nonnull final Path shardDirectory, @Nonnull final String segmentName, @Nonnull final Optional<String> tombstoneFileName) {
        this.shardDirectory = shardDirectory;
        this.segmentName = segmentName;
        this.tombstoneFileName = tombstoneFileName;
    }

    MultiThreadLock acquireReaderLock() throws IOException {
        return MultiThreadFileLockUtil.readLock(getDirectory(), SEGMENT_LOCK_FILENAME);
    }

    @Nonnull
    Path getShardDirectory() {
        return shardDirectory;
    }

    @Nonnull
    String getName() {
        return segmentName;
    }

    @Nonnull
    Path getDirectory() {
        return shardDirectory.resolve(segmentName);
    }

    @Nonnull
    Optional<Path> getTombstonePath() {
        if (!this.tombstoneFileName.isPresent()) {
            return Optional.absent();
        }
        return Optional.of(getDirectory().resolve(this.tombstoneFileName.get()));
    }

    @Nonnull
    Optional<FastBitSet> readTombstone() throws IOException {
        final Optional<Path> tombstonePath = getTombstonePath();
        if (!tombstonePath.isPresent()) {
            return Optional.absent();
        }
        final ByteBuffer byteBuffer = ByteBuffer.wrap(Files.readAllBytes(tombstonePath.get()));
        return Optional.of(FastBitSet.deserialize(byteBuffer));
    }

    @Nonnull
    String encodeAsLine() {
        if (this.tombstoneFileName.isPresent()) {
            return this.segmentName + "\t" + this.tombstoneFileName.get();
        } else {
            return this.segmentName;
        }
    }

    static SegmentInfo decodeFromLine(@Nonnull final Path shardDirectory, @Nonnull final String line) {
        final String[] tokens = line.split("\t");
        Preconditions.checkState((tokens.length == 1) || (tokens.length == 2));
        final String segmentName = tokens[0];
        final Optional<String> tombstoneName;
        if (tokens.length == 1) {
            tombstoneName = Optional.absent();
        } else {
            tombstoneName = Optional.of(tokens[1]);
        }
        return new SegmentInfo(shardDirectory, segmentName, tombstoneName);
    }

    @Override
    public int compareTo(@Nonnull final SegmentInfo that) {
        Preconditions.checkArgument(this.shardDirectory.equals(that.shardDirectory), "Don't compare segments in other shards");
        final int result = this.segmentName.compareTo(that.segmentName);
        if (result == 0) {
            Preconditions.checkArgument(this.tombstoneFileName.equals(that.tombstoneFileName), "Don't compare different version of the same segment");
        }
        return result;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if ((o == null) || (getClass() != o.getClass())) {
            return false;
        }
        final SegmentInfo that = (SegmentInfo) o;
        return Objects.equals(shardDirectory, that.shardDirectory) &&
                Objects.equals(segmentName, that.segmentName) &&
                Objects.equals(tombstoneFileName, that.tombstoneFileName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(shardDirectory, segmentName, tombstoneFileName);
    }
}

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
    private final int numDocs;
    private final Optional<String> tombstoneSetFileName;

    SegmentInfo(@Nonnull final Path shardDirectory, @Nonnull final String segmentName, final int numDocs, @Nonnull final Optional<String> tombstoneSetFileName) {
        this.shardDirectory = shardDirectory;
        this.segmentName = segmentName;
        this.numDocs = numDocs;
        this.tombstoneSetFileName = tombstoneSetFileName;
    }

    @Nonnull
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
    int getNumDocs() {
        return this.numDocs;
    }

    @Nonnull
    Path getDirectory() {
        return shardDirectory.resolve(segmentName);
    }

    @Nonnull
    Optional<Path> getTombstoneSetPath() {
        if (!this.tombstoneSetFileName.isPresent()) {
            return Optional.absent();
        }
        return Optional.of(getDirectory().resolve(this.tombstoneSetFileName.get()));
    }

    @Nonnull
    Optional<FastBitSet> readTombstoneSet() throws IOException {
        final Optional<Path> tombstoneSetPath = getTombstoneSetPath();
        if (!tombstoneSetPath.isPresent()) {
            return Optional.absent();
        }
        final ByteBuffer byteBuffer = ByteBuffer.wrap(Files.readAllBytes(tombstoneSetPath.get()));
        return Optional.of(FastBitSet.deserialize(byteBuffer));
    }

    @Nonnull
    String encodeAsLine() {
        String line = this.segmentName + "\t" + this.numDocs;
        if (this.tombstoneSetFileName.isPresent()) {
            line += "\t" + this.tombstoneSetFileName.get();
        }
        return line;
    }

    @Nonnull
    static SegmentInfo decodeFromLine(@Nonnull final Path shardDirectory, @Nonnull final String line) {
        final String[] tokens = line.split("\t");
        Preconditions.checkState((tokens.length == 2) || (tokens.length == 3));
        final String segmentName = tokens[0];
        final int numDocs = Integer.parseInt(tokens[1]);
        final Optional<String> tombstoneSetName;
        if (tokens.length == 2) {
            tombstoneSetName = Optional.absent();
        } else {
            tombstoneSetName = Optional.of(tokens[2]);
        }
        return new SegmentInfo(shardDirectory, segmentName, numDocs, tombstoneSetName);
    }

    @Override
    public int compareTo(@Nonnull final SegmentInfo that) {
        Preconditions.checkArgument(this.shardDirectory.equals(that.shardDirectory), "Don't compare segments in other shards");
        final int segmentNameDiff = segmentName.compareTo(that.segmentName);
        if (segmentNameDiff == 0) {
            Preconditions.checkArgument(this.tombstoneSetFileName.equals(that.tombstoneSetFileName), "Don't compare different version of the same segment");
        }
        final int sizeDiff = Integer.compare(this.getNumDocs(), that.getNumDocs());
        if (sizeDiff != 0) {
            return sizeDiff;
        }
        return segmentNameDiff;
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
                Objects.equals(numDocs, that.numDocs) &&
                Objects.equals(tombstoneSetFileName, that.tombstoneSetFileName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(shardDirectory, segmentName, numDocs, tombstoneSetFileName);
    }
}

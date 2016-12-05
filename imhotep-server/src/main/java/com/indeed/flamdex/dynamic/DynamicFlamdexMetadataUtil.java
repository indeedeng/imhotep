package com.indeed.flamdex.dynamic;

import com.google.common.base.Charsets;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.io.Files;
import com.indeed.flamdex.dynamic.locks.MultiThreadFileLockUtil;
import com.indeed.flamdex.dynamic.locks.MultiThreadLock;
import com.indeed.util.core.io.Closeables2;
import org.apache.log4j.Logger;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/**
 * @author michihiko
 */
final class DynamicFlamdexMetadataUtil {
    private static final Logger LOG = Logger.getLogger(DynamicFlamdexMetadataUtil.class);

    private static final String METADATA_FILENAME = "shard.metadata.txt";
    private static final String BUILD_SEGMENT_LOCK_FILENAME = "build.locck";

    private DynamicFlamdexMetadataUtil() {
    }

    // Don't forget to read-lock or write-lock the matadata.
    private static ImmutableList<SegmentInfo> readSegments(@Nonnull final MultiThreadLock lock, @Nonnull final Path directory) throws IOException {
        Preconditions.checkArgument(!lock.isClosed());
        return FluentIterable.from(Files.asCharSource(directory.resolve(METADATA_FILENAME).toFile(), Charsets.UTF_8).readLines()).transform(new Function<String, SegmentInfo>() {
            @Nullable
            @Override
            public SegmentInfo apply(@Nullable final String line) {
                return SegmentInfo.decodeFromLine(directory, line);
            }
        }).toList();
    }

    private static void writeSegments(@Nonnull final MultiThreadLock lock, @Nonnull final Path directory, @Nonnull final List<SegmentInfo> segmentInfos) throws IOException {
        Preconditions.checkArgument(!lock.isClosed());
        Preconditions.checkArgument(!lock.isShared());
        Files.asCharSink(directory.resolve(METADATA_FILENAME).toFile(), Charsets.UTF_8).writeLines(
                FluentIterable.from(segmentInfos).transform(new Function<SegmentInfo, String>() {
                    @Nullable
                    @Override
                    public String apply(@Nullable final SegmentInfo segmentInfo) {
                        return segmentInfo.encodeAsLine();
                    }
                }).toList());
    }

    @Nonnull
    static ImmutableList<SegmentReader> openSegmentReaders(@Nonnull final Path directory) throws IOException {
        try (final MultiThreadLock lock = MultiThreadFileLockUtil.readLock(directory, METADATA_FILENAME)) {
            final List<SegmentInfo> segmentInfos = readSegments(lock, directory);
            final ImmutableList.Builder<SegmentReader> segmentReaderBuilder = ImmutableList.builder();
            try {
                for (final SegmentInfo segmentInfo : segmentInfos) {
                    final SegmentReader segmentReader = new SegmentReader(segmentInfo);
                    segmentReaderBuilder.add(segmentReader);
                }
            } catch (final IOException e) {
                Closeables2.closeAll(segmentReaderBuilder.build(), LOG);
            }
            return segmentReaderBuilder.build();
        }
    }

    static void modifyMetadata(@Nonnull final Path directory, @Nonnull final Function<List<SegmentInfo>, List<SegmentInfo>> metadataUpdater) throws IOException {
        try (final MultiThreadLock lock = MultiThreadFileLockUtil.writeLock(directory, METADATA_FILENAME)) {
            final List<SegmentInfo> before = new ArrayList<>(readSegments(lock, directory));
            final List<SegmentInfo> after = Preconditions.checkNotNull(metadataUpdater.apply(before));
            writeSegments(lock, directory, after);
        }
    }

    @Nonnull
    static MultiThreadLock aquireSegmentBuildLock(@Nonnull final Path shardDirectory) throws IOException {
        return MultiThreadFileLockUtil.writeLock(shardDirectory, BUILD_SEGMENT_LOCK_FILENAME);
    }
}

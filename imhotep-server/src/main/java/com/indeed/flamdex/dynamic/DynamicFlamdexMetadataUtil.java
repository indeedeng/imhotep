package com.indeed.flamdex.dynamic;

import com.google.common.base.Charsets;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.io.Files;
import com.indeed.flamdex.dynamic.locks.MultiThreadFileLockUtil;
import com.indeed.flamdex.dynamic.locks.MultiThreadLock;
import com.indeed.util.core.io.Closeables2;
import org.apache.log4j.Logger;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

/**
 * @author michihiko
 */
final class DynamicFlamdexMetadataUtil {
    private static final Logger LOG = Logger.getLogger(DynamicFlamdexMetadataUtil.class);

    private static final String METADATA_FILENAME = "shard.metadata.txt";

    private DynamicFlamdexMetadataUtil() {
    }


    // Don't forget to read-lock or write-lock the matadata.
    private static List<String> readSegments(@Nonnull final MultiThreadLock lock, @Nonnull final Path directory) throws IOException {
        Preconditions.checkArgument(!lock.isClosed());
        return Files.asCharSource(directory.resolve(METADATA_FILENAME).toFile(), Charsets.UTF_8).readLines();
    }

    private static void writeSegments(@Nonnull final MultiThreadLock lock, @Nonnull final Path directory, @Nonnull final List<String> segments) throws IOException {
        Preconditions.checkArgument(!lock.isClosed());
        Preconditions.checkArgument(!lock.isShared());
        Files.asCharSink(directory.resolve(METADATA_FILENAME).toFile(), Charsets.UTF_8).writeLines(segments);
    }

    @Nonnull
    static List<SegmentReader> openSegmentReaders(@Nonnull final Path directory) throws IOException {
        try (final MultiThreadLock lock = MultiThreadFileLockUtil.readLock(directory, METADATA_FILENAME)) {
            final List<String> segments = readSegments(lock, directory);
            final ImmutableList.Builder<SegmentReader> segmentReaderBuilder = ImmutableList.builder();
            try {
                for (final String segment : segments) {
                    final SegmentReader segmentReader = new SegmentReader(directory, segment);
                    segmentReaderBuilder.add(segmentReader);
                }
            } catch (final IOException e) {
                Closeables2.closeAll(segmentReaderBuilder.build(), LOG);
            }
            return segmentReaderBuilder.build();
        }
    }

    static void modifyMetadata(@Nonnull final Path directory, @Nonnull final Function<List<String>, List<String>> metadataUpdater) throws IOException {
        try (final MultiThreadLock lock = MultiThreadFileLockUtil.writeLock(directory, METADATA_FILENAME)) {
            final List<String> before = readSegments(lock, directory);
            final List<String> after = Preconditions.checkNotNull(metadataUpdater.apply(before));
            writeSegments(lock, directory, after);
        }
    }
}

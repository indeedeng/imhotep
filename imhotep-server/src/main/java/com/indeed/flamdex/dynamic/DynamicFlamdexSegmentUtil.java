package com.indeed.flamdex.dynamic;

import com.google.common.base.Optional;
import com.indeed.flamdex.datastruct.FastBitSet;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

/**
 * @author michihiko
 */

final class DynamicFlamdexSegmentUtil {

    private static final String TOMBSTONE_FILENAME = "tombstoneSet.bin";

    private DynamicFlamdexSegmentUtil() {
    }

    @Nonnull
    static Optional<FastBitSet> readTombstoneSet(@Nonnull final Path segmentDirectory) throws IOException {
        final Path path = segmentDirectory.resolve(TOMBSTONE_FILENAME);
        if (Files.exists(path)) {
            final ByteBuffer byteBuffer = ByteBuffer.wrap(Files.readAllBytes(path));
            return Optional.of(FastBitSet.deserialize(byteBuffer));
        } else {
            return Optional.absent();
        }
    }

    static void writeTombstoneSet(@Nonnull final Path segmentDirectory, @Nonnull final FastBitSet tombstoneSet) throws IOException {
        final Path path = segmentDirectory.resolve(TOMBSTONE_FILENAME);
        Files.deleteIfExists(path);
        try (final ByteChannel byteChannel = Files.newByteChannel(path, StandardOpenOption.WRITE, StandardOpenOption.CREATE)) {
            byteChannel.write(tombstoneSet.serialize());
        }
    }
}

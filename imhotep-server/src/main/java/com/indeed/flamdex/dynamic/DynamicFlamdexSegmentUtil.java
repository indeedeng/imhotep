package com.indeed.flamdex.dynamic;

import com.indeed.flamdex.datastruct.FastBitSet;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.nio.channels.ByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.UUID;

/**
 * @author michihiko
 */
final class DynamicFlamdexSegmentUtil {
    private static final String TOMBSTONE_FILENAME = "tombstoneSet.%s.bin";

    private DynamicFlamdexSegmentUtil() {
    }

    @Nonnull
    public static String generateSegmentName() {
        return UUID.randomUUID().toString();
    }

    @Nonnull
    public static String outputTombstoneSet(@Nonnull final Path segmentDirectory, @Nonnull final String newSegmentName, @Nonnull final FastBitSet tombstoneSet) throws IOException {
        final String tombstoneFilename = String.format(TOMBSTONE_FILENAME, newSegmentName);
        final Path path = segmentDirectory.resolve(tombstoneFilename);

        try (final ByteChannel byteChannel = Files.newByteChannel(path, StandardOpenOption.WRITE, StandardOpenOption.CREATE)) {
            byteChannel.write(tombstoneSet.serialize());
        }
        return tombstoneFilename;
    }

}

/*
 * Copyright (C) 2018 Indeed Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */

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

package com.indeed.flamdex.dynamic;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Ordering;

import javax.annotation.Nonnull;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author michihiko
 */
final class DynamicShardMetadata implements Closeable {
    private static final String METADATA_FILENAME = "shard.metadata.txt";
    private static final String SEGMENT_LOCK_FILENAME = "reader.lock";

    private final AtomicBoolean closed;
    private final Path directory;

    private final MultiThreadFileLock fileLock;

    private DynamicShardMetadata(@Nonnull final Path directory, final boolean shared) throws IOException {
        this.directory = directory;
        this.fileLock = MultiThreadFileLock.lock(shared, directory, METADATA_FILENAME);
        this.closed = new AtomicBoolean(false);
    }

    @Nonnull
    Path getDirectory() {
        return this.directory;
    }

    @Nonnull
    private List<String> readSegments() throws IOException {
        final ImmutableList.Builder<String> segmentsBuilder = ImmutableList.builder();
        final FileChannel fileChannel = this.fileLock.getFileChannel();
        fileChannel.position(0);
        final InputStream inputStream = Channels.newInputStream(fileChannel);
        final InputStreamReader inputStreamReader = new InputStreamReader(inputStream);
        // Don't close the channel!!
        @SuppressWarnings("IOResourceOpenedButNotSafelyClosed")
        final BufferedReader br = new BufferedReader(inputStreamReader);
        while (true) {
            final String line = br.readLine();
            if (line == null) {
                break;
            }
            segmentsBuilder.add(line);
        }
        return segmentsBuilder.build();
    }

    private void writeSegments(@Nonnull final List<String> segments) throws IOException {
        final FileChannel fileChannel = this.fileLock.getFileChannel();
        fileChannel.truncate(0);
        final OutputStream outputStream = Channels.newOutputStream(fileChannel);
        final OutputStreamWriter outputStreamWriter = new OutputStreamWriter(outputStream);
        // Don't close the channel!!
        @SuppressWarnings("IOResourceOpenedButNotSafelyClosed")
        final BufferedWriter bufferedWriter = new BufferedWriter(outputStreamWriter);
        for (final String segment : Ordering.natural().sortedCopy(segments)) {
            bufferedWriter.write(segment);
            bufferedWriter.newLine();
        }
        bufferedWriter.flush();
    }

    boolean isClosed() {
        return closed.get();
    }

    @Override
    public void close() throws IOException {
        if (!this.closed.getAndSet(true)) {
            this.fileLock.close();
        }
    }

    @Nonnull
    MultiThreadFileLock readlockSegment(@Nonnull final String segment) throws IOException {
        return MultiThreadFileLock.readLock(this.directory, segment, SEGMENT_LOCK_FILENAME);
    }

    @Nonnull
    static List<SegmentReader> openSegmentReaders(@Nonnull final Path directory) throws IOException {
        try (final DynamicShardMetadata metadata = new DynamicShardMetadata(directory, true)) {
            final List<String> segments = metadata.readSegments();
            final ImmutableList.Builder<SegmentReader> segmentReaderBuilder = ImmutableList.builder();
            for (final String segment : segments) {
                try {
                    segmentReaderBuilder.add(new SegmentReader(metadata, segment));
                } catch (final IOException e) {
                    for (final SegmentReader segmentReader : segmentReaderBuilder.build()) {
                        segmentReader.close();
                    }
                    throw e;
                }
            }
            return segmentReaderBuilder.build();
        }
    }

    static void modifyMetadata(@Nonnull final Path directory, @Nonnull final Function<List<String>, List<String>> callback) throws IOException {
        try (final DynamicShardMetadata metadata = new DynamicShardMetadata(directory, false)) {
            final List<String> before = metadata.readSegments();
            final List<String> after = Preconditions.checkNotNull(callback.apply(before));
            metadata.writeSegments(after);
        }
    }
}

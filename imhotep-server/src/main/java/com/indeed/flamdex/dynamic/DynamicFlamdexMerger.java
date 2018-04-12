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

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicates;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.io.Closer;
import com.indeed.flamdex.api.DocIdStream;
import com.indeed.flamdex.api.IntTermIterator;
import com.indeed.flamdex.api.StringTermIterator;
import com.indeed.flamdex.datastruct.FastBitSet;
import com.indeed.flamdex.reader.FlamdexMetadata;
import com.indeed.flamdex.simple.SimpleFlamdexWriter;
import com.indeed.flamdex.writer.IntFieldWriter;
import com.indeed.flamdex.writer.StringFieldWriter;
import com.indeed.util.core.Throwables2;
import org.apache.log4j.Logger;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

/**
 * @author michihiko
 */

class DynamicFlamdexMerger implements Closeable {
    private static final Logger LOG = Logger.getLogger(DynamicFlamdexMerger.class);

    private class MergeTask implements Callable<Void> {
        private final List<MergeStrategy.Segment> segmentsToMerge;

        MergeTask(@Nonnull final List<MergeStrategy.Segment> segmentsToMerge) {
            this.segmentsToMerge = segmentsToMerge;
        }

        private class DocIdMapping {
            private final int newNumDocs;
            private final int[] newDocIds;
            private final int[] offset;

            private DocIdMapping(final int newNumDocs, @Nonnull final int[] newDocIds, @Nonnull final int[] offset) {
                this.newNumDocs = newNumDocs;
                this.newDocIds = newDocIds;
                this.offset = offset;
            }

            /**
             * @param segmentId      Index of the ald segment.
             * @param docIdInSegment Segment-local document ID of the document.
             * @return Segment-local ID of the document in the new segment, or -1 if it has been deleted.
             */
            int getNewDocId(final int segmentId, final int docIdInSegment) {
                return newDocIds[docIdInSegment + offset[segmentId]];
            }

            int getNewNumDocs() {
                return newNumDocs;
            }
        }

        @Nonnull
        private DocIdMapping mergeIndices(@Nonnull final Path outputPath, @Nonnull final List<SegmentReader> segmentReaders) throws IOException {
            final int numSegments = segmentReaders.size();
            Path dir = segmentReaders.get(0).getDirectory();
            if(dir != null) {
                dir = dir.getParent();
            }
            try (final DynamicFlamdexReader reader = new DynamicFlamdexReader(dir, segmentReaders)) {
                final int numDocs = reader.getNumDocs();
                final int[] newDocIds = new int[numDocs];

                final int[] offset = new int[numSegments + 1];
                for (int segmentIdx = 0; segmentIdx < segmentReaders.size(); segmentIdx++) {
                    offset[segmentIdx + 1] = offset[segmentIdx] + segmentReaders.get(segmentIdx).maxNumDocs();
                }
                int newNumDoc = 0;
                for (int segmentId = 0; segmentId < segmentReaders.size(); ++segmentId) {
                    final SegmentReader segmentReader = segmentReaders.get(segmentId);
                    for (int docId = 0; docId < segmentReaders.get(segmentId).maxNumDocs(); ++docId) {
                        if (segmentReader.isDeleted(docId)) {
                            newDocIds[docId + offset[segmentId]] = -1;
                        } else {
                            newDocIds[docId + offset[segmentId]] = newNumDoc;
                            newNumDoc++;
                        }
                    }
                }
                try (final SimpleFlamdexWriter writer = new SimpleFlamdexWriter(outputPath, newNumDoc)) {
                    final int[] docIdBuf = new int[256];
                    try (final DocIdStream docIdStream = reader.getDocIdStream()) {
                        for (final String field : reader.getIntFields()) {
                            try (
                                    final IntFieldWriter intFieldWriter = writer.getIntFieldWriter(field);
                                    final IntTermIterator intTermIterator = reader.getIntTermIterator(field)
                            ) {
                                while (intTermIterator.next()) {
                                    intFieldWriter.nextTerm(intTermIterator.term());
                                    docIdStream.reset(intTermIterator);
                                    while (true) {
                                        final int num = docIdStream.fillDocIdBuffer(docIdBuf);
                                        for (int i = 0; i < num; ++i) {
                                            final int docId = newDocIds[docIdBuf[i]];
                                            if (docId >= 0) {
                                                intFieldWriter.nextDoc(docId);
                                            }
                                        }
                                        if (num < docIdBuf.length) {
                                            break;
                                        }
                                    }
                                }
                            }
                        }
                        for (final String field : reader.getStringFields()) {
                            try (
                                    final StringFieldWriter stringFieldWriter = writer.getStringFieldWriter(field);
                                    final StringTermIterator stringTermIterator = reader.getStringTermIterator(field)
                            ) {
                                while (stringTermIterator.next()) {
                                    stringFieldWriter.nextTerm(stringTermIterator.term());
                                    docIdStream.reset(stringTermIterator);
                                    while (true) {
                                        final int num = docIdStream.fillDocIdBuffer(docIdBuf);
                                        for (int i = 0; i < num; ++i) {
                                            final int docId = newDocIds[docIdBuf[i]];
                                            if (docId >= 0) {
                                                stringFieldWriter.nextDoc(docId);
                                            }
                                        }
                                        if (num < docIdBuf.length) {
                                            break;
                                        }
                                    }
                                }
                            }
                        }
                    }
                    return new DocIdMapping(newNumDoc, newDocIds, offset);
                }
            }
        }

        @Override
        @Nullable
        public Void call() throws IOException {
            final Path newSegmentDirectory;
            try (final Closer closer = Closer.create()) {
                startMerge(segmentsToMerge);

                final List<SegmentReader> segmentReaders = new ArrayList<>(segmentsToMerge.size());
                for (final MergeStrategy.Segment segment : segmentsToMerge) {
                    segmentReaders.add(closer.register(new SegmentReader(segment.getSegmentDirectory())));
                }
                newSegmentDirectory = indexCommitter.newSegmentDirectory(true);

                if (LOG.isDebugEnabled()) {
                    LOG.debug("Start merge task "
                            + newSegmentDirectory.getFileName()
                            + " which merges "
                            + Joiner.on(',').join(FluentIterable.from(segmentsToMerge).transform(
                            new Function<MergeStrategy.Segment, String>() {
                                @Override
                                public String apply(final MergeStrategy.Segment segment) {
                                    return segment.getSegmentDirectory().getFileName().toString();
                                }
                            }
                    )));
                }

                final DocIdMapping docIdMapping = mergeIndices(newSegmentDirectory, segmentReaders);

                final FastBitSet tombstoneSet = new FastBitSet(docIdMapping.getNewNumDocs());

                // Wait segment builders / other merger
                indexCommitter.doWhileLockingSegment(new DynamicFlamdexIndexCommitter.CurrentSegmentConsumer() {
                    @Override
                    public void accept(@Nonnull final List<Path> currentSegmentPaths) throws IOException {
                        Preconditions.checkState(Iterables.all(
                                FluentIterable.from(segmentsToMerge).transform(MergeStrategy.Segment.SEGMENT_DIRECTORY_GETTER),
                                Predicates.in(currentSegmentPaths)));

                        // Update tombstoneSet.
                        // We've already removed documents which is already in the tombstone set when we opened segment readers,
                        // but since that time, writer could commit new segment which cause insertion on the tombstone set.
                        for (int segmentId = 0; segmentId < segmentsToMerge.size(); segmentId++) {
                            // The latest committed tombstone set for the segment
                            final Optional<FastBitSet> optionalLatestTombstoneSet = DynamicFlamdexSegmentUtil.readTombstoneSet(segmentsToMerge.get(segmentId).getSegmentDirectory());
                            if (!optionalLatestTombstoneSet.isPresent()) {
                                continue;
                            }
                            final FastBitSet latestTombstoneSet = optionalLatestTombstoneSet.get();
                            for (final FastBitSet.IntIterator it = latestTombstoneSet.iterator(); it.next(); ) {
                                final int newDocId = docIdMapping.getNewDocId(segmentId, it.getValue());
                                if (newDocId >= 0) {
                                    // If deleted document in the segment is in our new segment (i.e. newDocId is non-negative),
                                    // we should put it into tombstone set of new segment.
                                    tombstoneSet.set(newDocId);
                                }
                            }
                        }
                        if (!tombstoneSet.isEmpty()) {
                            DynamicFlamdexSegmentUtil.writeTombstoneSet(newSegmentDirectory, tombstoneSet);
                        }

                        indexCommitter.replaceSegmentsAndCommitIfPossible(
                                FluentIterable.from(segmentsToMerge)
                                        .transform(MergeStrategy.Segment.SEGMENT_DIRECTORY_GETTER)
                                        .toList(),
                                newSegmentDirectory
                        );
                    }
                });
            } catch (final Throwable e) {
                LOG.error("Failed to merge "
                        + Joiner.on(',').join(FluentIterable.from(segmentsToMerge).transform(
                        new Function<MergeStrategy.Segment, String>() {
                            @Override
                            public String apply(final MergeStrategy.Segment segment) {
                                return segment.getSegmentDirectory().getFileName().toString();
                            }
                        }
                )), e);
                abortMerge(segmentsToMerge);
                throw e;
            }
            finishMerge(segmentsToMerge);
            if (LOG.isDebugEnabled()) {
                LOG.debug("Finished merge task "
                        + newSegmentDirectory.getFileName()
                        + " which merges "
                        + Joiner.on(',').join(FluentIterable.from(segmentsToMerge).transform(
                        new Function<MergeStrategy.Segment, String>() {
                            @Override
                            public String apply(final MergeStrategy.Segment segment) {
                                return segment.getSegmentDirectory().getFileName().toString();
                            }
                        }
                )));
            }
            updated();
            return null;
        }
    }

    private final DynamicFlamdexIndexCommitter indexCommitter;
    private final MergeStrategy mergeStrategy;
    private final ExecutorService executorService;
    private final Set<Path> queuedSegments = new HashSet<>();
    private final List<Future<?>> futuresForJoin = new LinkedList<>();

    DynamicFlamdexMerger(@Nonnull final DynamicFlamdexIndexCommitter indexCommitter, @Nonnull final MergeStrategy mergeStrategy, @Nonnull final ExecutorService executorService) {
        this.indexCommitter = indexCommitter;
        this.mergeStrategy = mergeStrategy;
        this.executorService = executorService;
    }

    @Override
    public void close() throws IOException {
        try {
            join();
        } catch (final InterruptedException | ExecutionException e) {
            throw new IOException(e);
        }
    }

    /**
     * Called when we start to merge the segments, with the segments we're going to merge.
     */
    private synchronized void startMerge(@Nonnull final List<MergeStrategy.Segment> segments) {
        Preconditions.checkState(Iterables.all(
                FluentIterable.from(segments).transform(MergeStrategy.Segment.SEGMENT_DIRECTORY_GETTER),
                Predicates.in(queuedSegments)),
                "It looks there is another merger that already used this segment"
        );
    }

    /**
     * Called after we merged and committed successfully.
     */
    private synchronized void finishMerge(@Nonnull final List<MergeStrategy.Segment> segments) {
        queuedSegments.removeAll(FluentIterable.from(segments)
                .transform(MergeStrategy.Segment.SEGMENT_DIRECTORY_GETTER)
                .toList());
    }

    /**
     * Called after we abort merge, even if exception has been thrown during startMerge.
     */
    private synchronized void abortMerge(@Nonnull final List<MergeStrategy.Segment> segments) {
        queuedSegments.removeAll(FluentIterable.from(segments)
                .transform(MergeStrategy.Segment.SEGMENT_DIRECTORY_GETTER)
                .toList());
    }

    @Nullable
    private synchronized Future<?> firstIncompleteFuture() {
        final Iterator<Future<?>> iterator = futuresForJoin.iterator();
        while (iterator.hasNext()) {
            final Future<?> future = iterator.next();
            if (future.isDone()) {
                iterator.remove();
            } else {
                return future;
            }
        }
        return null;
    }

    /**
     * Wait all merge task to be finished. Allow merge tasks to spawn new merge tasks after the task has been finished.
     *
     * @throws InterruptedException
     * @throws ExecutionException
     */
    void join() throws InterruptedException, ExecutionException {
        Throwable throwable = null;
        while (true) {
            final Future<?> future = firstIncompleteFuture();
            if (future == null) {
                break;
            }
            try {
                future.get();
            } catch (final Throwable e) {
                if (throwable == null) {
                    throwable = e;
                } else {
                    throwable.addSuppressed(e);
                }
            }
        }
        queuedSegments.clear();
        if (throwable != null) {
            throw Throwables2.propagate(throwable, InterruptedException.class, ExecutionException.class);
        }
    }

    /**
     * Call this method whenever the set of the latest segments has been changed.
     * <p>
     * We have to take care about the consistency of data when we submits MergeTask to the ExecutorService
     * to prevent recursion of updated() in the same thread;
     * if executorService is something like SameThreadExecutorService, then
     * it curiously cause recursion on update() method in the same thread.
     */
    synchronized void updated() throws IOException {
        indexCommitter.doWhileLockingSegment(new DynamicFlamdexIndexCommitter.CurrentSegmentConsumer() {
            @Override
            public void accept(@Nonnull final List<Path> currentSegmentPaths) throws IOException {
                final List<MergeStrategy.Segment> availableSegments = new ArrayList<>();
                for (final Path segmentPath : currentSegmentPaths) {
                    if (!queuedSegments.contains(segmentPath)) {
                        final FlamdexMetadata metadata = FlamdexMetadata.readMetadata(segmentPath);
                        availableSegments.add(new MergeStrategy.Segment(segmentPath, metadata.getNumDocs()));
                    }
                }
                final ImmutableList.Builder<List<MergeStrategy.Segment>> splitSegmentsBuilder = ImmutableList.builder();

                for (final Collection<MergeStrategy.Segment> segments : mergeStrategy.splitSegmentsToMerge(new ArrayList<>(availableSegments))) {
                    if (segments.size() <= 1) {
                        continue;
                    }

                    for (final MergeStrategy.Segment segment : segments) {
                        Preconditions.checkState(availableSegments.remove(segment),
                                "Same segment is selected twice, or non-available segment is selected.");
                    }
                    splitSegmentsBuilder.add(ImmutableList.copyOf(segments));
                }

                final List<List<MergeStrategy.Segment>> splitSegments = splitSegmentsBuilder.build();

                queuedSegments.addAll(FluentIterable.from(Iterables.concat(splitSegments))
                        .transform(MergeStrategy.Segment.SEGMENT_DIRECTORY_GETTER)
                        .toList());

                for (final List<MergeStrategy.Segment> splitSegment : splitSegments) {
                    try {
                        futuresForJoin.add(executorService.submit(new MergeTask(splitSegment)));
                    } catch (final Throwable e) {
                        LOG.error("Failed to submit merge tasks", e);
                        queuedSegments.removeAll(FluentIterable.from(splitSegment)
                                .transform(MergeStrategy.Segment.SEGMENT_DIRECTORY_GETTER)
                                .toList());
                    }
                }
            }
        });
    }
}

package com.indeed.flamdex.dynamic;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.io.Closer;
import com.indeed.flamdex.api.DocIdStream;
import com.indeed.flamdex.api.IntTermIterator;
import com.indeed.flamdex.api.StringTermIterator;
import com.indeed.flamdex.datastruct.FastBitSet;
import com.indeed.flamdex.simple.SimpleFlamdexWriter;
import com.indeed.flamdex.writer.IntFieldWriter;
import com.indeed.flamdex.writer.StringFieldWriter;
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
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.locks.Lock;

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

            int getNewDocId(final int segmentId, final int docIdInSegment) {
                return newDocIds[docIdInSegment + offset[segmentId]];
            }

            int getNewNumDocs() {
                return newNumDocs;
            }
        }

        @Nonnull
        private DocIdMapping mergeIndices(@Nonnull final Path outputPath, @Nonnull final List<SegmentReader> segmentReaders) throws IOException {
            {
                final StringBuilder logString = new StringBuilder();
                logString.append("Generating ").append(outputPath.getFileName()).append(" from");
                for (final SegmentReader segmentReader : segmentReaders) {
                    logString
                            .append(' ')
                            .append(segmentReader.getDirectory().getFileName())
                            .append('(')
                            .append(segmentReader.maxNumDocs())
                            .append(')');
                }
                LOG.info(logString.toString());
            }

            final int numSegments = segmentReaders.size();
            try (final DynamicFlamdexReader reader = new DynamicFlamdexReader(segmentReaders.get(0).getDirectory().getParent(), segmentReaders)) {
                final int numDocs = reader.getNumDocs();
                final int[] newDocIds = new int[numDocs];

                final int[] offset = new int[numSegments + 1];
                for (int i = 0; i < segmentReaders.size(); i++) {
                    offset[i + 1] = offset[i] + segmentReaders.get(i).maxNumDocs();
                }
                int newNumDoc = 0;
                for (int i = 0; i < segmentReaders.size(); ++i) {
                    final SegmentReader segmentReader = segmentReaders.get(i);
                    for (int id = 0; id < segmentReaders.get(i).maxNumDocs(); ++id) {
                        if (segmentReader.isDeleted(id)) {
                            newDocIds[id + offset[i]] = -1;
                        } else {
                            newDocIds[id + offset[i]] = newNumDoc++;
                        }
                    }
                }
                try (final SimpleFlamdexWriter writer = new SimpleFlamdexWriter(outputPath, newNumDoc)) {
                    final int[] docIdBuf = new int[256];
                    try (final DocIdStream docIdStream = reader.getDocIdStream()) {
                        for (final String field : reader.getIntFields()) {
                            try (
                                    final IntFieldWriter intFieldWriter = writer.getIntFieldWriter(field);
                                    final IntTermIterator intTermIterator = reader.getIntTermIterator(field);
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
                                    final StringTermIterator stringTermIterator = reader.getStringTermIterator(field);
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
            if (!startMerge(segmentsToMerge)) {
                return null;
            }

            try (final Closer closer = Closer.create()) {
                final List<SegmentReader> segmentReaders = new ArrayList<>(segmentsToMerge.size());
                for (final MergeStrategy.Segment segment : segmentsToMerge) {
                    segmentReaders.add(closer.register(new SegmentReader(segment.getSegmentDirectory())));
                }
                final Path newSegmentDirectory = shardCommitter.newSegmentDirectory();
                final DocIdMapping docIdMapping = mergeIndices(newSegmentDirectory, segmentReaders);

                final FastBitSet tombstoneSet = new FastBitSet(docIdMapping.getNewNumDocs());

                // Wait segment builders / other merger
                final Lock lock = shardCommitter.getChangeSegmentsLock();
                lock.lock();
                try {
                    // Update tombstoneSet
                    for (int segmentId = 0; segmentId < segmentsToMerge.size(); segmentId++) {
                        final Optional<FastBitSet> optionalLatestTombstoneSet = DynamicFlamdexSegmentUtil.readTombstoneSet(segmentsToMerge.get(segmentId).getSegmentDirectory());
                        if (!optionalLatestTombstoneSet.isPresent()) {
                            continue;
                        }
                        final FastBitSet latestTombstoneSet = optionalLatestTombstoneSet.get();
                        for (final FastBitSet.IntIterator it = latestTombstoneSet.iterator(); it.next(); ) {
                            final int newDocId = docIdMapping.getNewDocId(segmentId, it.getValue());
                            if (newDocId >= 0) {
                                tombstoneSet.set(newDocId);
                            }
                        }
                    }
                    if (!tombstoneSet.isEmpty()) {
                        DynamicFlamdexSegmentUtil.writeTombstoneSet(newSegmentDirectory, tombstoneSet);
                    }

                    shardCommitter.replaceSegments(
                            FluentIterable.from(segmentsToMerge).transform(new Function<MergeStrategy.Segment, Path>() {
                                @Override
                                public Path apply(final MergeStrategy.Segment segment) {
                                    return segment.getSegmentDirectory();
                                }
                            }).toList(),
                            newSegmentDirectory
                    );
                    shardCommitter.commit();
                } finally {
                    lock.unlock();
                }
            } catch (final Throwable e) {
                LOG.error("", e);
                throw e;
            }
            updated();
            finishMerge(segmentsToMerge);
            return null;
        }
    }

    private final DynamicFlamdexShardCommitter shardCommitter;
    private final MergeStrategy mergeStrategy;
    private final ExecutorService executorService;
    private final Set<Path> queuedSegments = new HashSet<>();
    private final Set<Path> processingSegments = new HashSet<>();
    private final List<Future<?>> futuresForCancellation = new LinkedList<>();

    DynamicFlamdexMerger(@Nonnull final DynamicFlamdexShardCommitter shardCommitter, @Nonnull final MergeStrategy mergeStrategy, @Nonnull final ExecutorService executorService) {
        this.shardCommitter = shardCommitter;
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

    private synchronized boolean startMerge(@Nonnull final List<MergeStrategy.Segment> segments) {
        boolean isValid = true;
        for (final MergeStrategy.Segment segment : segments) {
            isValid &= queuedSegments.remove(segment.getSegmentDirectory());
        }
        if (isValid) {
            for (final MergeStrategy.Segment segment : segments) {
                processingSegments.add(segment.getSegmentDirectory());
            }
        }
        return isValid;
    }

    private synchronized void finishMerge(@Nonnull final List<MergeStrategy.Segment> segments) {
        for (final MergeStrategy.Segment segment : segments) {
            processingSegments.remove(segment.getSegmentDirectory());
        }
    }

    @Nullable
    private synchronized Future<?> firstIncompleteFuture() {
        final Iterator<Future<?>> iterator = futuresForCancellation.iterator();
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
        Exception exception = null;
        while (true) {
            final Future<?> future = firstIncompleteFuture();
            if (future == null) {
                break;
            }
            try {
                future.get();
            } catch (final InterruptedException | ExecutionException e) {
                if (exception == null) {
                    exception = e;
                } else {
                    exception.addSuppressed(e);
                }
            }
        }
        if (exception != null) {
            if (exception instanceof InterruptedException) {
                throw (InterruptedException) exception;
            } else {
                throw (ExecutionException) exception;
            }
        }
    }

    void cancel() {
        while (true) {
            final Future<?> future = firstIncompleteFuture();
            if (future == null) {
                break;
            }
            future.cancel(true);
        }
        queuedSegments.clear();
    }

    synchronized boolean isDone() {
        return (firstIncompleteFuture() == null) && queuedSegments.isEmpty();
    }

    synchronized void updated() throws IOException {
        final Lock lock = shardCommitter.getChangeSegmentsLock();
        lock.lock();
        try {
            final SortedSet<MergeStrategy.Segment> availableSegments = new TreeSet<>();
            for (final Path segmentPath : shardCommitter.getCurrentSegmentPaths()) {
                if (!queuedSegments.contains(segmentPath) && !processingSegments.contains(segmentPath)) {
                    try (final SegmentReader segmentReader = new SegmentReader(segmentPath)) {
                        availableSegments.add(new MergeStrategy.Segment(segmentPath, segmentReader.maxNumDocs()));
                    }
                }
            }
            final Collection<? extends Collection<MergeStrategy.Segment>> segmentsToMerge = mergeStrategy.splitSegmentsToMerge(new TreeSet<>(availableSegments));
            for (final Collection<MergeStrategy.Segment> segments : segmentsToMerge) {
                if (segments.size() <= 1) {
                    continue;
                }

                final ImmutableList.Builder<Path> segmentsToMergeBuilder = ImmutableList.builder();
                for (final MergeStrategy.Segment segment : segments) {
                    Preconditions.checkState(availableSegments.remove(segment), "Same segment is selected twice.");
                    segmentsToMergeBuilder.add(segment.getSegmentDirectory());
                }
                final List<Path> segmentNamesToMerge = segmentsToMergeBuilder.build();
                futuresForCancellation.add(executorService.submit(new MergeTask(ImmutableList.copyOf(segments))));
                queuedSegments.addAll(segmentNamesToMerge);
            }
        } catch (final Throwable e) {
            LOG.error("", e);
            throw e;
        } finally {
            lock.unlock();
        }
    }
}

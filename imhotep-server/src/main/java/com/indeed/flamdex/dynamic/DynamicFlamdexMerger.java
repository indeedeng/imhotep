package com.indeed.flamdex.dynamic;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.io.Closer;
import com.indeed.flamdex.MemoryFlamdex;
import com.indeed.flamdex.api.DocIdStream;
import com.indeed.flamdex.api.IntTermIterator;
import com.indeed.flamdex.api.StringTermIterator;
import com.indeed.flamdex.datastruct.FastBitSet;
import com.indeed.flamdex.dynamic.locks.MultiThreadLock;
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
import java.util.Comparator;
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

class DynamicFlamdexMerger implements Closeable {
    private static final Logger LOG = Logger.getLogger(DynamicFlamdexMerger.class);

    private class MergeTask implements Callable<Void> {
        private final List<String> segmentsToMerge;

        MergeTask(@Nonnull final List<String> segmentsToMerge) {
            this.segmentsToMerge = segmentsToMerge;
        }

        private class IntermediateState {
            private final MemoryFlamdex mergedIndex;
            private final int[] newDocIds;
            private final int[] offset;

            IntermediateState(@Nonnull final MemoryFlamdex mergedIndex, @Nonnull final int[] newDocIds, @Nonnull final int[] offset) {
                this.mergedIndex = mergedIndex;
                this.newDocIds = newDocIds;
                this.offset = offset;
            }

            int getNewDocId(final int segmentId, final int docIdInSegment) {
                return newDocIds[docIdInSegment + offset[segmentId]];
            }

            int getNewNumDocs() {
                return mergedIndex.getNumDocs();
            }

            public MemoryFlamdex getMergedIndex() {
                return mergedIndex;
            }
        }

        @Nonnull
        private IntermediateState mergeIndices(@Nonnull final List<SegmentReader> segmentReaders) throws IOException {
            final int numSegments = segmentReaders.size();
            try (final DynamicFlamdexReader reader = new DynamicFlamdexReader(shardDirectory, segmentReaders)) {
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
                final MemoryFlamdex writer = new MemoryFlamdex();
                writer.setNumDocs(newNumDoc);

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

                return new IntermediateState(writer, newDocIds, offset);
            }
        }

        @Override
        @Nullable
        public Void call() throws IOException {
            if (!startMerge(segmentsToMerge)) {
                return null;
            }
            final List<SegmentInfo> segmentInfosToMerge = FluentIterable.from(DynamicFlamdexMetadataUtil.getMetadata(shardDirectory))
                    .filter(new Predicate<SegmentInfo>() {
                        @Override
                        public boolean apply(final SegmentInfo segmentInfo) {
                            return segmentsToMerge.contains(segmentInfo.getName());
                        }
                    }).toSortedList(new Comparator<SegmentInfo>() {
                        @Override
                        public int compare(final SegmentInfo o1, final SegmentInfo o2) {
                            return Integer.compare(segmentsToMerge.indexOf(o1.getName()), segmentsToMerge.indexOf(o2.getName()));
                        }
                    });
            if (segmentInfosToMerge.size() != segmentsToMerge.size()) {
                throw new IOException("Failed to read segment information");
            }
            try (final Closer closer = Closer.create()) {
                final List<SegmentReader> segmentReaders = new ArrayList<>(segmentInfosToMerge.size());
                for (final SegmentInfo segmentInfo : segmentInfosToMerge) {
                    segmentReaders.add(closer.register(new SegmentReader(segmentInfo)));
                }

                final String newSegmentName = DynamicFlamdexSegmentUtil.generateSegmentName();
                {
                    final StringBuilder logString = new StringBuilder();
                    logString.append("Making ").append(newSegmentName).append(" from");
                    for (final SegmentInfo segment : segmentInfosToMerge) {
                        logString
                                .append(' ')
                                .append(segment.getName())
                                .append('(')
                                .append(segment.getNumDocs())
                                .append(')');
                    }
                    LOG.info(logString.toString());
                }

                final IntermediateState intermediateResult = mergeIndices(segmentReaders);

                final Path segmentDirectory = shardDirectory.resolve(newSegmentName);
                try (final SimpleFlamdexWriter flamdexWriter = new SimpleFlamdexWriter(segmentDirectory, intermediateResult.getNewNumDocs())) {
                    SimpleFlamdexWriter.writeFlamdex(intermediateResult.getMergedIndex(), flamdexWriter);
                }

                final FastBitSet tombstone = new FastBitSet(intermediateResult.getNewNumDocs());

                // Wait segment builder to be finished
                // Inside this try-block, there are no changes for the segment metadata (including tombstone).
                try (final MultiThreadLock stopSegmentBuilderLock = DynamicFlamdexMetadataUtil.acquireSegmentBuildLock(shardDirectory)) {
                    // Update tombstone
                    final ImmutableList.Builder<SegmentInfo> resultBuilder = ImmutableList.builder();
                    for (final SegmentInfo latestSegmentInfo : DynamicFlamdexMetadataUtil.getMetadata(shardDirectory)) {
                        final int idx = segmentsToMerge.indexOf(latestSegmentInfo.getName());
                        if (idx == -1) {
                            resultBuilder.add(latestSegmentInfo);
                            continue;
                        }
                        final Optional<FastBitSet> optionalLatestTombstoneSet = latestSegmentInfo.readTombstoneSet();
                        if (!optionalLatestTombstoneSet.isPresent()) {
                            continue;
                        }
                        final FastBitSet latestTombstone = optionalLatestTombstoneSet.get();
                        for (int docId = 0; docId < latestTombstone.size(); ++docId) {
                            if (latestTombstone.get(docId)) {
                                final int newDocId = intermediateResult.getNewDocId(idx, docId);
                                if (newDocId >= 0) {
                                    tombstone.set(newDocId);
                                }
                            }
                        }
                    }

                    final Optional<String> tombstoneFilename;
                    if (tombstone.isEmpty()) {
                        tombstoneFilename = Optional.absent();
                    } else {
                        final Path segmentPath = shardDirectory.resolve(newSegmentName);
                        tombstoneFilename = Optional.of(DynamicFlamdexSegmentUtil.outputTombstoneSet(segmentPath, newSegmentName, tombstone));
                    }
                    resultBuilder.add(new SegmentInfo(shardDirectory, newSegmentName, intermediateResult.getNewNumDocs(), tombstoneFilename));

                    DynamicFlamdexMetadataUtil.modifyMetadata(shardDirectory, new Function<List<SegmentInfo>, List<SegmentInfo>>() {
                        @Override
                        public List<SegmentInfo> apply(final List<SegmentInfo> segmentInfos) {
                            return resultBuilder.build();
                        }
                    });
                }
            }
            updated();
            return null;
        }
    }

    private final Path shardDirectory;
    private final MergeStrategy mergeStrategy;
    private final ExecutorService executorService;
    private final Set<String> queuedSegments = new HashSet<>();
    private final Set<String> dequeuedSegments = new HashSet<>();
    private final List<Future<?>> futuresForCancellation = new LinkedList<>();

    DynamicFlamdexMerger(@Nonnull final Path shardDirectory, @Nonnull final MergeStrategy mergeStrategy, @Nonnull final ExecutorService executorService) throws IOException {
        this.shardDirectory = shardDirectory;
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

    private synchronized boolean startMerge(final List<String> segments) {
        boolean isValid = true;
        for (final String segment : segments) {
            isValid &= queuedSegments.remove(segment);
        }
        if (isValid) {
            dequeuedSegments.addAll(segments);
        }
        return isValid;
    }

    @Nullable
    private synchronized Future<?> firstIncompleteFuture(){
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

    void cancel() throws InterruptedException, ExecutionException {
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
        final SortedSet<SegmentInfo> availableSegments = new TreeSet<>();
        for (final SegmentInfo segmentInfo : DynamicFlamdexMetadataUtil.getMetadata(shardDirectory)) {
            if (!queuedSegments.contains(segmentInfo.getName()) && !dequeuedSegments.contains(segmentInfo.getName())) {
                availableSegments.add(segmentInfo);
            }
        }

        final Collection<? extends Collection<SegmentInfo>> segmentInfoSetsToMerge = mergeStrategy.splitSegmentsToMerge(new TreeSet<>(availableSegments));
        for (final Collection<SegmentInfo> segmentsToMerge : segmentInfoSetsToMerge) {
            Preconditions.checkState(availableSegments.containsAll(segmentsToMerge), "Invalid segment is selected.");
        }
        for (final Collection<SegmentInfo> segmentInfos : segmentInfoSetsToMerge) {
            if (segmentInfos.size() <= 1) {
                continue;
            }
            final ImmutableList.Builder<String> segmentsToMergeBuilder = ImmutableList.builder();
            for (final SegmentInfo segmentInfo : segmentInfos) {
                Preconditions.checkState(availableSegments.remove(segmentInfo), "Same segment is selected twice.");
                segmentsToMergeBuilder.add(segmentInfo.getName());
            }
            final List<String> segmentsToMerge = segmentsToMergeBuilder.build();
            futuresForCancellation.add(executorService.submit(new MergeTask(segmentsToMerge)));
            queuedSegments.addAll(segmentsToMerge);
        }
    }
}

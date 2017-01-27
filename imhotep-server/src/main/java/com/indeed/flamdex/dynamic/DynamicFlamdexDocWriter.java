package com.indeed.flamdex.dynamic;

import com.google.common.base.Optional;
import com.google.common.util.concurrent.MoreExecutors;
import com.indeed.flamdex.MemoryFlamdex;
import com.indeed.flamdex.api.DocIdStream;
import com.indeed.flamdex.api.IntTermIterator;
import com.indeed.flamdex.api.StringTermIterator;
import com.indeed.flamdex.datastruct.FastBitSet;
import com.indeed.flamdex.dynamic.locks.MultiThreadLock;
import com.indeed.flamdex.query.BooleanOp;
import com.indeed.flamdex.query.Query;
import com.indeed.flamdex.search.FlamdexSearcher;
import com.indeed.flamdex.simple.SimpleFlamdexWriter;
import com.indeed.flamdex.writer.FlamdexDocument;
import com.indeed.flamdex.writer.IntFieldWriter;
import com.indeed.flamdex.writer.StringFieldWriter;
import com.indeed.util.core.io.Closeables2;
import it.unimi.dsi.fastutil.ints.IntIterator;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import org.apache.log4j.Logger;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.locks.Lock;

/**
 * @author michihiko
 */

public class DynamicFlamdexDocWriter implements DeletableFlamdexDocWriter {
    private static final Logger LOG = Logger.getLogger(DynamicFlamdexDocWriter.class);

    public static final int FORMAT_VERSION = 3;

    private static final int DOC_ID_BUFFER_SIZE = 128;

    private final DynamicFlamdexShardCommitter shardCommitter;

    private final MultiThreadLock writerLock;
    private final DynamicFlamdexMerger merger;
    private MemoryFlamdex memoryFlamdex;
    private FlamdexSearcher flamdexSearcher;
    private final IntSet removedDocIds;
    private final List<Query> removeQueries;
    private final int[] docIdBuf = new int[DOC_ID_BUFFER_SIZE];

    public DynamicFlamdexDocWriter(@Nonnull final Path datasetDirectory, @Nonnull final String shardDirectoryPrefix) throws IOException {
        this(datasetDirectory, shardDirectoryPrefix, null, null, null);
    }

    public DynamicFlamdexDocWriter(@Nonnull final Path datasetDirectory, @Nonnull final String shardDirectoryPrefix, @Nonnull final Path latestShardDirectory) throws IOException {
        this(datasetDirectory, shardDirectoryPrefix, latestShardDirectory, null, null);
    }

    public DynamicFlamdexDocWriter(@Nonnull final Path datasetDirectory, @Nonnull final String shardDirectoryPrefix, @Nonnull final MergeStrategy mergeStrategy, @Nullable final ExecutorService executorService) throws IOException {
        this(datasetDirectory, shardDirectoryPrefix, null, mergeStrategy, executorService);
    }

    /**
     * Creates shard from {@code latestShardDirectory}, and write into {@code datasetDirectory}/{@code shardDirectoryPrefix}.version.timestamp.
     * Do merge if {@code mergeStrategy} is nonnull, and use other thread to merge segments if {@code executorService} is passed.
     */
    public DynamicFlamdexDocWriter(@Nonnull final Path datasetDirectory, @Nonnull final String shardDirectoryPrefix, @Nullable final Path latestShardDirectory, @Nullable final MergeStrategy mergeStrategy, @Nullable final ExecutorService executorService) throws IOException {
        this.writerLock = DynamicFlamdexShardUtil.acquireWriterLock(datasetDirectory, shardDirectoryPrefix);
        this.shardCommitter = new DynamicFlamdexShardCommitter(datasetDirectory, shardDirectoryPrefix, latestShardDirectory);
        if (mergeStrategy == null) {
            this.merger = null;
        } else {
            if (executorService == null) {
                this.merger = new DynamicFlamdexMerger(shardCommitter, mergeStrategy, MoreExecutors.sameThreadExecutor());
            } else {
                this.merger = new DynamicFlamdexMerger(shardCommitter, mergeStrategy, executorService);
            }
        }
        this.removedDocIds = new IntOpenHashSet();
        this.removeQueries = new ArrayList<>();
        renew();
    }

    private void renew() throws IOException {
        if (this.memoryFlamdex != null) {
            this.memoryFlamdex.close();
        }
        this.memoryFlamdex = new MemoryFlamdex();
        this.flamdexSearcher = new FlamdexSearcher(this.memoryFlamdex);
        this.removedDocIds.clear();
        this.removeQueries.clear();
    }

    private void compactCurrentSegment() throws IOException {
        if (this.removedDocIds.isEmpty()) {
            return;
        }
        final int numDocs = this.memoryFlamdex.getNumDocs();
        final int[] newDocId = new int[numDocs];
        for (final IntIterator iterator = this.removedDocIds.iterator(); iterator.hasNext(); ) {
            newDocId[iterator.nextInt()] = -1;
        }
        int nextNumDocs = 0;
        for (int i = 0; i < numDocs; ++i) {
            if (newDocId[i] == 0) {
                newDocId[i] = nextNumDocs++;
            }
        }

        final MemoryFlamdex nextWriter = new MemoryFlamdex();
        nextWriter.setNumDocs(nextNumDocs);

        try (final DocIdStream docIdStream = this.memoryFlamdex.getDocIdStream()) {
            for (final String field : this.memoryFlamdex.getIntFields()) {
                try (
                        final IntFieldWriter intFieldWriter = nextWriter.getIntFieldWriter(field);
                        final IntTermIterator intTermIterator = this.memoryFlamdex.getIntTermIterator(field)
                ) {
                    while (intTermIterator.next()) {
                        intFieldWriter.nextTerm(intTermIterator.term());
                        docIdStream.reset(intTermIterator);
                        while (true) {
                            final int num = docIdStream.fillDocIdBuffer(this.docIdBuf);
                            for (int i = 0; i < num; ++i) {
                                final int docId = newDocId[this.docIdBuf[i]];
                                if (docId >= 0) {
                                    intFieldWriter.nextDoc(docId);
                                }
                            }
                            if (num < this.docIdBuf.length) {
                                break;
                            }
                        }
                    }
                }
            }
            for (final String field : this.memoryFlamdex.getStringFields()) {
                try (
                        final StringFieldWriter stringFieldWriter = nextWriter.getStringFieldWriter(field);
                        final StringTermIterator stringTermIterator = this.memoryFlamdex.getStringTermIterator(field)
                ) {
                    while (stringTermIterator.next()) {
                        stringFieldWriter.nextTerm(stringTermIterator.term());
                        docIdStream.reset(stringTermIterator);
                        while (true) {
                            final int num = docIdStream.fillDocIdBuffer(this.docIdBuf);
                            for (int i = 0; i < num; ++i) {
                                final int docId = newDocId[this.docIdBuf[i]];
                                if (docId >= 0) {
                                    stringFieldWriter.nextDoc(docId);
                                }
                            }
                            if (num < this.docIdBuf.length) {
                                break;
                            }
                        }
                    }
                }
            }
        } catch (final IOException e) {
            Closeables2.closeQuietly(this.memoryFlamdex, LOG);
            Closeables2.closeQuietly(nextWriter, LOG);
            throw e;
        }
        this.memoryFlamdex.close();
        this.removedDocIds.clear();
        this.memoryFlamdex = nextWriter;
        this.flamdexSearcher = new FlamdexSearcher(this.memoryFlamdex);
    }

    @Nonnull
    private Path buildSegment(final long version) throws IOException {
        final Path newSegmentPath = shardCommitter.newSegmentDirectory();
        try (final SimpleFlamdexWriter flamdexWriter = new SimpleFlamdexWriter(newSegmentPath, memoryFlamdex.getNumDocs())) {
            SimpleFlamdexWriter.writeFlamdex(memoryFlamdex, flamdexWriter);
        }

        final Lock lock = shardCommitter.getChangeSegmentsLock();
        lock.lock();
        try {
            final Map<Path, FastBitSet> updatedTombstoneSets = new HashMap<>();
            // Make tombstone for the new segment
            if (!removedDocIds.isEmpty()) {
                final FastBitSet tombstoneSet = new FastBitSet(memoryFlamdex.getNumDocs());
                for (final Integer removedDocId : removedDocIds) {
                    tombstoneSet.set(removedDocId);
                }
                updatedTombstoneSets.put(newSegmentPath, tombstoneSet);
            }
            // Make tombstoneSet for old segments
            if (!removeQueries.isEmpty()) {
                final Query query = Query.newBooleanQuery(BooleanOp.OR, removeQueries);
                for (final Path segmentPath : shardCommitter.getCurrentSegmentPaths()) {
                    try (final SegmentReader segmentReader = new SegmentReader(segmentPath)) {
                        final Optional<FastBitSet> updatedTombstoneSet = segmentReader.getUpdatedTombstoneSet(query);
                        if (updatedTombstoneSet.isPresent()) {
                            updatedTombstoneSets.put(segmentPath, updatedTombstoneSet.get());
                        }
                    }
                }
            }
            shardCommitter.addSegment(newSegmentPath, updatedTombstoneSets);
            return shardCommitter.commit(version);
        } finally {
            lock.unlock();
        }
    }

    @Nonnull
    public Optional<Path> commit(final long version) throws IOException {
        return commit(version, true);
    }

    @Nonnull
    public Optional<Path> commit(final long version, final boolean compact) throws IOException {
        if (compact) {
            compactCurrentSegment();
        }
        if ((memoryFlamdex.getNumDocs() == 0) && removeQueries.isEmpty()) {
            return Optional.absent();
        }
        final Path newShardDirectory = buildSegment(version);
        renew();
        if (merger != null) {
            merger.updated();
        }
        return Optional.of(newShardDirectory);
    }

    @Override
    public void close() throws IOException {
        Closeables2.closeAll(LOG, memoryFlamdex, merger, writerLock, shardCommitter);
    }

    @Override
    public void addDocument(@Nonnull final FlamdexDocument doc) throws IOException {
        memoryFlamdex.addDocument(doc);
    }

    @Override
    public void deleteDocuments(@Nonnull final Query query) throws IOException {
        removeQueries.add(query);
        final FastBitSet removed = flamdexSearcher.search(query);
        for (final FastBitSet.IntIterator iterator = removed.iterator(); iterator.next(); ) {
            removedDocIds.add(iterator.getValue());
        }
    }
}

package com.indeed.flamdex.dynamic;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.io.Closer;
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
import java.util.List;
import java.util.concurrent.ExecutorService;

/**
 * @author michihiko
 */

public class DynamicFlamdexDocWriter implements DeletableFlamdexDocWriter {
    private static final Logger LOG = Logger.getLogger(DynamicFlamdexDocWriter.class);

    private static final int DOC_ID_BUFFER_SIZE = 128;

    private final DynamicFlamdexIndexCommitter indexCommitter;

    private final MultiThreadLock writerLock;
    private final DynamicFlamdexMerger merger;
    private MemoryFlamdex memoryFlamdex;
    private FlamdexSearcher flamdexSearcher;
    private final IntSet removedDocIds;
    private final List<Query> removeQueries;
    private final int[] docIdBuf = new int[DOC_ID_BUFFER_SIZE];

    /**
     * Creates index from {@code latestIndexDirectory}, and write into {@code datasetDirectory}/{@code indexDirectoryPrefix}.version.timestamp.
     * Do merge if {@code mergeStrategy} is nonnull, and use {@code executorService} to merge segments.
     * If {@code latestIndexDirectory} is given, the writer writes on top of the index.
     * If the latestIndexDirectory is dynamic index, then the writer treat each segments in the index as the latest segments.
     * Otherwise, the writer treat it as single segment.
     */
    private DynamicFlamdexDocWriter(
            @Nonnull final Path datasetDirectory,
            @Nonnull final String indexDirectoryPrefix,
            @Nullable final Long latestVersion,
            @Nullable final Path latestIndexDirectory,
            @Nullable final MergeStrategy mergeStrategy,
            @Nullable final ExecutorService executorService
    ) throws IOException {
        final Closer closerOnFailure = Closer.create();
        try {
            this.writerLock = closerOnFailure.register(DynamicFlamdexIndexUtil.acquireWriterLock(datasetDirectory, indexDirectoryPrefix));
            this.indexCommitter = closerOnFailure.register(
                    new DynamicFlamdexIndexCommitter(datasetDirectory, indexDirectoryPrefix, latestVersion, latestIndexDirectory)
            );
            if (mergeStrategy == null) {
                this.merger = null;
                //noinspection VariableNotUsedInsideIf
                if (executorService != null) {
                    LOG.warn("Ignore executorService because mergeStrategy is not given.");
                }
            } else {
                Preconditions.checkNotNull(executorService, "mergeStrategy is passed, but executorService is null.");
                this.merger = closerOnFailure.register(new DynamicFlamdexMerger(indexCommitter, mergeStrategy, executorService));
            }
            this.removedDocIds = new IntOpenHashSet();
            this.removeQueries = new ArrayList<>();
            renew();
        } catch (final Throwable e) {
            Closeables2.closeQuietly(closerOnFailure, LOG);
            throw e;
        }
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
        final Path newSegmentDirectory = indexCommitter.newSegmentDirectory();
        // Output the segment
        try (final SimpleFlamdexWriter flamdexWriter = new SimpleFlamdexWriter(newSegmentDirectory, memoryFlamdex.getNumDocs())) {
            SimpleFlamdexWriter.writeFlamdex(memoryFlamdex, flamdexWriter);
        }
        // Write tombstone set for the new segment
        if (!removedDocIds.isEmpty()) {
            final FastBitSet tombstoneSet = new FastBitSet(memoryFlamdex.getNumDocs());
            for (final Integer removedDocId : removedDocIds) {
                tombstoneSet.set(removedDocId);
            }
            DynamicFlamdexSegmentUtil.writeTombstoneSet(newSegmentDirectory, tombstoneSet);
        }

        final Query query = removeQueries.isEmpty() ? null : Query.newBooleanQuery(BooleanOp.OR, removeQueries);
        return indexCommitter.addSegmentWithDeletionAndCommit(version, newSegmentDirectory, query);
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
        final Path newIndexDirectory = buildSegment(version);
        renew();
        if (merger != null) {
            merger.updated();
        }
        return Optional.of(newIndexDirectory);
    }

    @Override
    public void close() throws IOException {
        Closeables2.closeAll(LOG, memoryFlamdex, merger, indexCommitter, writerLock);
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

    public static Builder builder() {
        return new Builder();
    }

    @SuppressWarnings("ParameterHidesMemberVariable")
    public static class Builder {
        private Path datasetDirectory;
        private String indexDirectoryPrefix;
        private Long latestVersion = null;
        private Path latestIndexDirectory = null;
        private MergeStrategy mergeStrategy = null;
        private ExecutorService executorService = null;

        private Builder() {
        }

        public Builder setDatasetDirectory(@Nonnull final Path datasetDirectory) {
            this.datasetDirectory = datasetDirectory;
            return this;
        }

        public Builder setIndexDirectoryPrefix(@Nonnull final String indexDirectoryPrefix) {
            this.indexDirectoryPrefix = indexDirectoryPrefix;
            return this;
        }

        public Builder setLatestIndex(final long latestVersion, @Nonnull final Path latestIndexDirectory) {
            this.latestVersion = latestVersion;
            this.latestIndexDirectory = latestIndexDirectory;
            return this;
        }

        public Builder setMergeStrategy(@Nonnull final MergeStrategy mergeStrategy) {
            this.mergeStrategy = mergeStrategy;
            return this;
        }

        public Builder setExecutorService(@Nonnull final ExecutorService executorService) {
            this.executorService = executorService;
            return this;
        }

        public DynamicFlamdexDocWriter build() throws IOException {
            return new DynamicFlamdexDocWriter(
                    Preconditions.checkNotNull(datasetDirectory),
                    Preconditions.checkNotNull(indexDirectoryPrefix),
                    latestVersion,
                    latestIndexDirectory,
                    mergeStrategy,
                    executorService
            );
        }
    }
}

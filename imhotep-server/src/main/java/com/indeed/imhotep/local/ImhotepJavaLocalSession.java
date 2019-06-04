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
package com.indeed.imhotep.local;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.indeed.flamdex.api.FlamdexReader;
import com.indeed.flamdex.api.IntValueLookup;
import com.indeed.flamdex.reader.FlamdexMetadata;
import com.indeed.flamdex.simple.SimpleFlamdexReader;
import com.indeed.flamdex.simple.SimpleFlamdexWriter;
import com.indeed.flamdex.utils.ShardMetadataUtils;
import com.indeed.imhotep.ImhotepMemoryPool;
import com.indeed.imhotep.MemoryReservationContext;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.imhotep.api.ImhotepSession;
import com.indeed.imhotep.metrics.Constant;
import com.indeed.imhotep.metrics.Count;
import com.indeed.imhotep.protobuf.Operator;
import com.indeed.imhotep.service.CachedFlamdexReader;
import com.indeed.util.core.reference.SharedReference;
import com.indeed.util.core.shell.PosixFileOperations;
import org.apache.log4j.Logger;
import org.joda.time.Interval;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.net.URI;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

public class ImhotepJavaLocalSession extends ImhotepLocalSession {

    private static final Logger log = Logger.getLogger(ImhotepJavaLocalSession.class);

    private final String optimizedIndexesDir;
    private File optimizationLog;

    private FlamdexReader originalReader;
    private SharedReference<FlamdexReader> originalReaderRef;

    public ImhotepJavaLocalSession(final String sessionId, final FlamdexReader flamdexReader, @Nullable final Interval shardTimeRange)
        throws ImhotepOutOfMemoryException {
        this(sessionId, flamdexReader, shardTimeRange, null,
             new MemoryReservationContext(new ImhotepMemoryPool(Long.MAX_VALUE)), null);
    }

    public ImhotepJavaLocalSession(final String sessionId,
                                   final FlamdexReader flamdexReader,
                                   @Nullable final Interval shardTimeRange,
                                   final String optimizedIndexDirectory,
                                   final MemoryReservationContext memory,
                                   final AtomicLong tempFileSizeBytesLeft)
        throws ImhotepOutOfMemoryException {

        super(sessionId, flamdexReader, shardTimeRange, memory, tempFileSizeBytesLeft);

        this.optimizedIndexesDir = optimizedIndexDirectory;
    }

    /*
     * record structure to store the info necessary to rebuild the
     * DynamicMetrics after one or more optimizes and a reset
     */
    private static class OptimizationRecord implements Serializable {
        private static final long serialVersionUID = 1L;
        public long time;
        List<String> intFieldsMerged;
        List<String> stringFieldsMerged;
        URI shardLocation;
        List<ShardMergeInfo> mergedShards;
    }

    public static class ShardMergeInfo implements Serializable {
        private static final long serialVersionUID = 1L;
        int numDocs;
        Map<String, DynamicMetric> dynamicMetrics;
        int[] newDocIdToOldDocId;
    }

    /*
     * Finds a good place to store the new, optimized shard and opens a
     * SimpleFlamdexWriter to it.
     */
    private SimpleFlamdexWriter createNewTempWriter(final int maxDocs) throws IOException {
        final Path tempIdxDir;
        final String newShardName;
        final Path newShardDir;

        newShardName = "temp." + UUID.randomUUID().toString();
        tempIdxDir = Paths.get(this.optimizedIndexesDir);
        newShardDir =tempIdxDir.resolve(newShardName);
        Files.createDirectories(newShardDir);

        return new SimpleFlamdexWriter(newShardDir, maxDocs);
    }

    /* wrapper for SimpleFlamdexReader which deletes the on disk data on close() */
    private static class AutoDeletingReader extends SimpleFlamdexReader {

        public AutoDeletingReader(final Path directory,
                                  final int numDocs,
                                  final Collection<String> intFields,
                                  final Collection<String> stringFields,
                                  final Config config) {
            super(directory, numDocs, intFields, stringFields, config);
        }

        public static AutoDeletingReader open(@Nonnull final Path directory) throws IOException {
            return open(directory, new Config());
        }

        public static AutoDeletingReader open(@Nonnull final Path directory, final Config config) throws IOException {
            // TODO: this is a copy-paste of SimpleFlamdexReader::open
            final FlamdexMetadata metadata = FlamdexMetadata.readMetadata(directory);

            final List<Path> paths = new ArrayList<>();
            try (final DirectoryStream<Path> dirStream = Files.newDirectoryStream(directory)) {
                for (final Path path : dirStream) {
                    paths.add(path);
                }
            }

            final ShardMetadataUtils.AllFields fields = ShardMetadataUtils.getFieldsFromFlamdexFiles(paths);
            final Collection<String> intFields = fields.intFields;
            final Collection<String> stringFields = fields.strFields;
            if (config.isWriteBTreesIfNotExisting()) {
                final Set<String> pathNames = Sets.newHashSet();
                for (Path path : paths) {
                    pathNames.add(path.getFileName().toString());
                }
                buildIntBTrees(directory, pathNames, Lists.newArrayList(intFields));
                buildStringBTrees(directory, pathNames, Lists.newArrayList(stringFields));
            }
            final AutoDeletingReader result =
                    new AutoDeletingReader(
                            directory,
                            metadata.getNumDocs(),
                            intFields,
                            stringFields,
                            config);
            if (config.isWriteCardinalityIfNotExisting()) {
                // try to build and store cache
                result.buildAndWriteCardinalityCache(true);
            }
            return result;
        }

        @Override
        public void close() throws IOException {
            super.close();
            PosixFileOperations.rmrf(this.directory);
        }

    }

    /* Tweak to ObjectOutputStream which allows it to append to an existing file */
    private static class AppendingObjectOutputStream extends ObjectOutputStream {

        public AppendingObjectOutputStream(final OutputStream out) throws IOException {
            super(out);
        }

        @Override
        protected void writeStreamHeader() throws IOException {
            // do not write a header, but reset:
            reset();
        }

    }

    @Override
    public synchronized void rebuildAndFilterIndexes(
            final String groupsName,
            final List<String> intFields,
            final List<String> stringFields)
        throws ImhotepOutOfMemoryException {

        final long time = System.currentTimeMillis();

        /* pop off all the stats, they will be repushed after the optimization */
        final List<String> statsCopy = new ArrayList<>(metricStack.getStatCommands());
        while (metricStack.getNumStats() > 0) {
            this.popStat();
        }
        metricStack.clearStatCommands();

        final Path writerOutputDir;
        try {
            try (MemoryReservationContext rewriterMemory = new MemoryReservationContext(memory)) {
                final IndexReWriter rewriter = new IndexReWriter(Collections.singletonList(this), this, groupsName, rewriterMemory);
                final OptimizationRecord record;
                final ShardMergeInfo info;
                try (SimpleFlamdexWriter w = createNewTempWriter(this.numDocs)) {
                    rewriter.optimizeIndices(intFields, stringFields, w);
                    writerOutputDir = w.getOutputDirectory();
                }

                /*
                 * save a record of the merge, so it can be unwound later if the
                 * shards are reset
                 */
                if (optimizationLog == null) {
                    optimizationLog = new File(this.optimizedIndexesDir, UUID.randomUUID().toString() + ".optimization_log");
                }
                try (ObjectOutputStream oos = this.optimizationLog.exists() ? new AppendingObjectOutputStream(new FileOutputStream(this.optimizationLog,
                        true)) : // plain ObjectOutputStream does not append correctly
                        new ObjectOutputStream(new FileOutputStream(this.optimizationLog, true))) {

                    record = new OptimizationRecord();

                    record.time = time;
                    record.intFieldsMerged = intFields;
                    record.stringFieldsMerged = stringFields;
                    record.shardLocation = writerOutputDir.toUri();
                    record.mergedShards = new ArrayList<>();

                    info = new ShardMergeInfo();
                    info.numDocs = this.flamdexReader.getNumDocs();
                    info.dynamicMetrics = this.getDynamicMetrics();
                    info.newDocIdToOldDocId = rewriter.getPerSessionMappings().get(0);
                    record.mergedShards.add(info);

                    oos.writeObject(record);
                }

                /* use rebuilt structures */
                namedGroupLookups = rewriter.getNewGroupLookups();

                for (final DynamicMetric dm : this.dynamicMetrics.values()) {
                    memory.releaseMemory(dm.memoryUsed());
                }
                for (final DynamicMetric dm : rewriter.getDynamicMetrics().values()) {
                    rewriterMemory.hoist(dm.memoryUsed());
                }
                this.dynamicMetrics = rewriter.getDynamicMetrics();
            }

            // replace flamdexReader pointers, but keep the originals in case
            // there is a reset() call
            if (this.originalReader == null) {
                this.originalReader = this.flamdexReader;
            }
            if (this.originalReaderRef == null) {
                this.originalReaderRef = this.flamdexReaderRef;
            } else {
                // close the unnecessary optimized index
                this.flamdexReaderRef.close();
            }

            final FlamdexReader flamdex = AutoDeletingReader.open(writerOutputDir);
            this.flamdexReader = new CachedFlamdexReader(
                    new MemoryReservationContext(memory),
                    flamdex);
            this.flamdexReaderRef = SharedReference.create(this.flamdexReader);
        } catch (final IOException e) {
            throw Throwables.propagate(e);
        }

        // alter tracking fields to reflect the removal of group 0 docs
        this.numDocs = this.flamdexReader.getNumDocs();

        // push the stats back on
        for (final String stat : statsCopy) {
            if ("pop".equals(stat)) {
                this.popStat();
            } else {
                this.pushStat(stat);
            }
        }
    }

    /*
     * Resets the Flamdex readers to the original un-optimized versions and
     * constructs the DynamicMetrics to match what they should be if no
     * optimization had taken place.
     *
     * The GroupLookup does not have to be reconstructed since it will be set to
     * a constant value as a result of a reset() call
     */
    private synchronized void resetOptimizedReaders() throws ImhotepOutOfMemoryException {
        ObjectInputStream ois = null;
        final ArrayList<OptimizationRecord> records = new ArrayList<>();
        final long memoryUse;
        final ArrayList<String> statsCopy;

        /* check if this session has been optimized */
        if (this.originalReader == null) {
            return;
        }

        /* check for space in memory */
        memoryUse = optimizationLog == null ? 0 : this.optimizationLog.length();
        if (!this.memory.claimMemory(memoryUse)) {
            throw newImhotepOutOfMemoryException();
        }

        /* pop off all the stats, they will be repushed after the flamdex reset */
        statsCopy = new ArrayList<>(metricStack.getStatCommands());
        while (metricStack.getNumStats() > 0) {
            this.popStat();
        }
        metricStack.clearStatCommands();

        if (optimizationLog != null) {
            /* read in all the optimization records */
            try {
                ois = new ObjectInputStream(new FileInputStream(this.optimizationLog));
                while (true) {
                    /*
                     * adds the records so the last written record is first in the
                     * list
                     */
                    records.add(0, (OptimizationRecord) ois.readObject());
                }
            } catch (final EOFException e) {
                // read all the records
                try {
                    if (ois != null) {
                        ois.close();
                        this.optimizationLog.delete();
                    }
                } catch (final IOException e1) {
                    /* do nothing */
                    e.printStackTrace();
                }
            } catch (final ClassNotFoundException | IOException e) {
                throw newRuntimeException(e);
            } finally {
                /* the log is no longer needed, so remove it */
                this.optimizationLog.delete();
            }
        }

        /* reconstruct the dynamic metrics */
        Map<String, DynamicMetric> newMetrics;
        Map<String, DynamicMetric> oldMetrics;
        int numNewDocs;
        int numOldDocs;
        newMetrics = this.dynamicMetrics;
        numNewDocs = this.flamdexReader.getNumDocs();
        for (final OptimizationRecord opRec : records) {
            final int[] newToOldIdMapping = opRec.mergedShards.get(0).newDocIdToOldDocId;
            oldMetrics = opRec.mergedShards.get(0).dynamicMetrics;
            numOldDocs = opRec.mergedShards.get(0).numDocs;

            for (final Map.Entry<String, DynamicMetric> e : newMetrics.entrySet()) {
                DynamicMetric oldMetric = oldMetrics.get(e.getKey());
                final DynamicMetric newMetric = e.getValue();
                if (oldMetric == null) {
                    oldMetric = new DynamicMetric(numOldDocs);
                }
                final DynamicMetric.Editor editor = oldMetric.getEditor();
                for (int i = 0; i < numNewDocs; i++) {
                    final int oldId = newToOldIdMapping[i];
                    final int value = newMetric.lookupSingleVal(i);
                    editor.set(oldId, value);
                }

                oldMetrics.put(e.getKey(), oldMetric);
            }
            numNewDocs = numOldDocs;
            newMetrics = oldMetrics;
        }

        /* adjust the memory tracking */
        for (final DynamicMetric dm : this.dynamicMetrics.values()) {
            memory.releaseMemory(dm.memoryUsed());
        }
        for (final DynamicMetric dm : newMetrics.values()) {
            memory.claimMemory(dm.memoryUsed());
        }
        this.dynamicMetrics = newMetrics;

        try {
            /* close temp index reader */
            this.flamdexReaderRef.close();
        } catch (final IOException e) {
            log.error("[" + getSessionId() + "] Could not close optimized reader");
        }

        /* reopen the original flamdex readers */
        this.flamdexReader = this.originalReader;
        this.flamdexReaderRef = this.originalReaderRef;
        this.originalReader = null;
        this.originalReaderRef = null;

        this.numDocs = this.flamdexReader.getNumDocs();

        /* push the stats back on */
        for (final String stat : statsCopy) {
            if ("pop".equals(stat)) {
                this.popStat();
            } else {
                this.pushStat(stat);
            }
        }

        /* release the memory used by the log reading */
        this.memory.releaseMemory(memoryUse);

    }

    @Override
    public synchronized void resetGroups(final String groupsName) throws ImhotepOutOfMemoryException {
        // TODO: When do we actually want to reset optimized readers?
        // TODO: Do we actually care? It only affects model builders.
        resetOptimizedReaders();
        resetGroupsTo(groupsName, 1);
    }

    @Override
    protected void addGroupStats(final GroupLookup docIdToGroup, final List<String> stat, final long[] partialResult) throws ImhotepOutOfMemoryException {
        if (docIdToGroup.isFilteredOut()) {
            return;
        }

        final int[] docIdBuffer = memoryPool.getIntBuffer(ImhotepLocalSession.BUFFER_SIZE, true);
        final int[] docGroupBuffer = memoryPool.getIntBuffer(ImhotepLocalSession.BUFFER_SIZE, true);
        final long[] valueBuffer = memoryPool.getLongBuffer(ImhotepLocalSession.BUFFER_SIZE, true);

        try (final MetricStack stack = new MetricStack()) {
            final IntValueLookup lookup = stack.push(stat);
            updateGroupStatsAllDocs(
                    lookup,
                    partialResult,
                    docIdToGroup,
                    docGroupBuffer,
                    docIdBuffer,
                    valueBuffer
            );
        }
        memoryPool.returnIntBuffer(docIdBuffer);
        memoryPool.returnIntBuffer(docGroupBuffer);
        memoryPool.returnLongBuffer(valueBuffer);
    }

    private static void updateGroupStatsAllDocs(final IntValueLookup statLookup,
                                                final long[]         results,
                                                final GroupLookup    docIdToGroup,
                                                final int[]          docGrpBuffer,
                                                final int[]          docIdBuf,
                                                final long[]         valBuf) {
        // populate new group stats
        final int numDocs = docIdToGroup.size();
        for (int start = 0; start < numDocs; start += BUFFER_SIZE) {
            final int n = Math.min(BUFFER_SIZE, numDocs - start);
            for (int i = 0; i < n; i++) {
                docIdBuf[i] = start + i;
            }
            docIdToGroup.fillDocGrpBuffer(docIdBuf, docGrpBuffer, n);
            updateGroupStatsDocIdBuf(statLookup, results, docGrpBuffer, docIdBuf, valBuf, n);
        }
    }

    static void updateGroupStatsDocIdBuf(final IntValueLookup statLookup,
                                         final long[] results,
                                         final int[] docGrpBuffer,
                                         final int[] docIdBuf,
                                         final long[] valBuf,
                                         final int n) {
        /* This is a hacky optimization, but probably worthwhile, since Count is
         * such a common stat. Rather than have Count fill up an array with ones
         * and then add each in turn to our results, we just increment the
         * results directly. */
        if (statLookup instanceof Count) {
            for (int i = 0; i < n; i++) {
                results[docGrpBuffer[i]] += 1;
            }
        } else if (statLookup instanceof Constant) {
            final long value = ((Constant) statLookup).getValue();
            for (int i = 0; i < n; i++) {
                results[docGrpBuffer[i]] += value;
            }
        } else {
            statLookup.lookup(docIdBuf, valBuf, n);
            for (int i = 0; i < n; i++) {
                results[docGrpBuffer[i]] += valBuf[i];
            }
        }
        results[0] = 0; // clearing value for filtered-out group.
    }

    @Override
    protected void tryClose() {
        /* clean up the optimization log */
        if ((this.optimizationLog != null) && optimizationLog.exists()) {
            this.optimizationLog.delete();
        }
        try {
            /* close temp readers, if there are any */
            if (this.originalReader != null) {
                this.flamdexReaderRef.close();
                this.flamdexReader = this.originalReader;
                this.flamdexReaderRef = this.originalReaderRef;
            }
        } catch (final IOException e) {
            log.error("[" + getSessionId() + "] Could not close optimized reader");
        }

        super.tryClose();
    }
}

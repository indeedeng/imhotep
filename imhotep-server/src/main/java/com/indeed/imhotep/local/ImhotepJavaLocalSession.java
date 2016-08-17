/*
 * Copyright (C) 2014 Indeed Inc.
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

import com.google.common.collect.Lists;
import com.indeed.flamdex.api.FlamdexReader;
import com.indeed.flamdex.api.IntValueLookup;
import com.indeed.flamdex.api.RawFlamdexReader;
import com.indeed.flamdex.reader.FlamdexMetadata;
import com.indeed.flamdex.simple.SimpleFlamdexReader;
import com.indeed.flamdex.simple.SimpleFlamdexWriter;
import com.indeed.imhotep.ImhotepMemoryPool;
import com.indeed.imhotep.MemoryReservationContext;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.imhotep.service.CachedFlamdexReader;
import com.indeed.imhotep.service.RawCachedFlamdexReader;
import com.indeed.util.core.reference.SharedReference;
import com.indeed.util.core.shell.PosixFileOperations;
import org.apache.log4j.Logger;

import javax.annotation.Nonnull;
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
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

public class ImhotepJavaLocalSession extends ImhotepLocalSession {

    static final Logger log = Logger.getLogger(ImhotepJavaLocalSession.class);

    private final String optimizedIndexesDir;
    private final File optimizationLog;

    private FlamdexReader originalReader;
    private SharedReference<FlamdexReader> originalReaderRef;

    public ImhotepJavaLocalSession(final FlamdexReader flamdexReader)
        throws ImhotepOutOfMemoryException {
        this(flamdexReader, null,
             new MemoryReservationContext(new ImhotepMemoryPool(Long.MAX_VALUE)), null);
    }

    public ImhotepJavaLocalSession(final FlamdexReader flamdexReader,
                                   String optimizedIndexDirectory,
                                   final MemoryReservationContext memory,
                                   AtomicLong tempFileSizeBytesLeft)
        throws ImhotepOutOfMemoryException {

        super(flamdexReader, memory, tempFileSizeBytesLeft);

        this.optimizedIndexesDir = optimizedIndexDirectory;
        this.optimizationLog =
            new File(this.optimizedIndexesDir, UUID.randomUUID().toString() + ".optimization_log");
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
    private SimpleFlamdexWriter createNewTempWriter(int maxDocs) throws IOException {
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

        public AutoDeletingReader(Path directory,
                                  int numDocs,
                                  Collection<String> intFields,
                                  Collection<String> stringFields,
                                  boolean useMMapMetrics,
                                  boolean useMMapDocIdStream) {
            super(directory, numDocs, intFields, stringFields, useMMapMetrics, useMMapDocIdStream);
        }

        public static AutoDeletingReader open(Path directory) throws IOException {
            return open(directory, new Config());
        }

        public static AutoDeletingReader open(Path directory, Config config) throws IOException {
            final FlamdexMetadata metadata = FlamdexMetadata.readMetadata(directory);
            final Collection<String> intFields = scan(directory, ".intterms");
            final Collection<String> stringFields = scan(directory, ".strterms");
            if (config.isWriteBTreesIfNotExisting()) {
                buildIntBTrees(directory, Lists.newArrayList(intFields));
                buildStringBTrees(directory, Lists.newArrayList(stringFields));
            }
            return new AutoDeletingReader(directory, metadata.numDocs, intFields, stringFields,
                                          config.isUseMMapMetrics(), config.isUseMMapDocIdStream());
        }

        @Override
        public void close() throws IOException {
            super.close();
            PosixFileOperations.rmrf(this.directory);
        }

    }

    /* Tweak to ObjectOutputStream which allows it to append to an existing file */
    private static class AppendingObjectOutputStream extends ObjectOutputStream {

        public AppendingObjectOutputStream(OutputStream out) throws IOException {
            super(out);
        }

        @Override
        protected void writeStreamHeader() throws IOException {
            // do not write a header, but reset:
            reset();
        }

    }

    @Override
    public synchronized void rebuildAndFilterIndexes(@Nonnull final List<String> intFields,
                                                     @Nonnull final List<String> stringFields)
        throws ImhotepOutOfMemoryException {
        final IndexReWriter rewriter;
        final ObjectOutputStream oos;
        final SimpleFlamdexWriter w;
        final ArrayList<String> statsCopy;

        long time = System.currentTimeMillis();

        /* pop off all the stats, they will be repushed after the optimization */
        statsCopy = new ArrayList<String>(this.statCommands);
        while (this.numStats > 0) {
            this.popStat();
        }
        this.statCommands.clear();

        MemoryReservationContext rewriterMemory = new MemoryReservationContext(memory);
        rewriter = new IndexReWriter(Arrays.asList(this), this, rewriterMemory);
        try {
            w = createNewTempWriter(this.numDocs);
            rewriter.optimizeIndices(intFields, stringFields, w);
            w.close();

            /*
             * save a record of the merge, so it can be unwound later if the
             * shards are reset
             */
            if (this.optimizationLog.exists()) {
                /* plain ObjectOutputStream does not append correctly */
                oos = new AppendingObjectOutputStream(new FileOutputStream(this.optimizationLog,
                                                                           true));
            } else {
                oos = new ObjectOutputStream(new FileOutputStream(this.optimizationLog, true));
            }

            OptimizationRecord record = new OptimizationRecord();

            record.time = time;
            record.intFieldsMerged = intFields;
            record.stringFieldsMerged = stringFields;
            record.shardLocation = w.getOutputDirectory().toUri();
            record.mergedShards = new ArrayList<>();

            ShardMergeInfo info = new ShardMergeInfo();
            info.numDocs = this.flamdexReader.getNumDocs();
            info.dynamicMetrics = this.getDynamicMetrics();
            info.newDocIdToOldDocId = rewriter.getPerSessionMappings().get(0);
            record.mergedShards.add(info);

            oos.writeObject(record);
            oos.close();

            /* use rebuilt structures */
            memory.releaseMemory(this.docIdToGroup.memoryUsed());
            rewriterMemory.hoist(rewriter.getNewGroupLookup().memoryUsed());
            this.docIdToGroup = rewriter.getNewGroupLookup();

            for (DynamicMetric dm : this.dynamicMetrics.values()) {
                memory.releaseMemory(dm.memoryUsed());
            }
            for (DynamicMetric dm : rewriter.getDynamicMetrics().values()) {
                rewriterMemory.hoist(dm.memoryUsed());
            }
            this.dynamicMetrics = rewriter.getDynamicMetrics();

            /* release memory used by the index rewriter */
            rewriterMemory.close();

            /*
             * replace flamdexReader pointers, but keep the originals in case
             * there is a reset() call
             */
            if (this.originalReader == null) {
                this.originalReader = this.flamdexReader;
            }
            if (this.originalReaderRef == null) {
                this.originalReaderRef = this.flamdexReaderRef;
            } else {
                /* close the unnecessary optimized index */
                this.flamdexReaderRef.close();
            }

            FlamdexReader flamdex = AutoDeletingReader.open(w.getOutputDirectory());
            if (flamdex instanceof RawFlamdexReader) {
                this.flamdexReader =
                        new RawCachedFlamdexReader(new MemoryReservationContext(memory),
                                                   (RawFlamdexReader) flamdex, null, null,
                                                   null);
            } else {
                this.flamdexReader =
                        new CachedFlamdexReader(new MemoryReservationContext(memory), flamdex,
                                                null, null, null);
            }
            this.flamdexReaderRef = SharedReference.create(this.flamdexReader);

            /* alter tracking fields to reflect the removal of group 0 docs */
            this.numDocs = this.flamdexReader.getNumDocs();
            this.groupDocCount[0] = 0;

            /* push the stats back on */
            for (String stat : statsCopy) {
                if ("pop".equals(stat)) {
                    this.popStat();
                } else {
                    this.pushStat(stat);
                }
            }

        } catch (IOException e) {
            throw new RuntimeException(e);
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
        ArrayList<OptimizationRecord> records = new ArrayList<OptimizationRecord>();
        final long memoryUse;
        final ArrayList<String> statsCopy;

        /* check if this session has been optimized */
        if (this.originalReader == null) {
            return;
        }

        /* check for space in memory */
        memoryUse = this.optimizationLog.length();
        if (!this.memory.claimMemory(memoryUse)) {
            throw new ImhotepOutOfMemoryException();
        }

        /* pop off all the stats, they will be repushed after the flamdex reset */
        statsCopy = new ArrayList<String>(this.statCommands);
        while (this.numStats > 0) {
            this.popStat();
        }
        this.statCommands.clear();

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
        } catch (EOFException e) {
            // read all the records
            try {
                if (ois != null) {
                    ois.close();
                    this.optimizationLog.delete();
                }
            } catch (IOException e1) {
                /* do nothing */
                e.printStackTrace();
            }
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            /* the log is no longer needed, so remove it */
            this.optimizationLog.delete();
        }

        /* reconstruct the dynamic metrics */
        Map<String, DynamicMetric> newMetrics;
        Map<String, DynamicMetric> oldMetrics;
        int numNewDocs;
        int numOldDocs;
        newMetrics = this.dynamicMetrics;
        numNewDocs = this.flamdexReader.getNumDocs();
        for (OptimizationRecord opRec : records) {
            int[] newToOldIdMapping = opRec.mergedShards.get(0).newDocIdToOldDocId;
            oldMetrics = opRec.mergedShards.get(0).dynamicMetrics;
            numOldDocs = opRec.mergedShards.get(0).numDocs;

            for (Map.Entry<String, DynamicMetric> e : newMetrics.entrySet()) {
                DynamicMetric oldMetric = oldMetrics.get(e.getKey());
                DynamicMetric newMetric = e.getValue();
                if (oldMetric == null) {
                    oldMetric = new DynamicMetric(numOldDocs);
                }
                for (int i = 0; i < numNewDocs; i++) {
                    int oldId = newToOldIdMapping[i];
                    int value = newMetric.lookupSingleVal(i);
                    oldMetric.set(oldId, value);
                }

                oldMetrics.put(e.getKey(), oldMetric);
            }
            numNewDocs = numOldDocs;
            newMetrics = oldMetrics;
        }

        /* adjust the memory tracking */
        for (DynamicMetric dm : this.dynamicMetrics.values()) {
            memory.releaseMemory(dm.memoryUsed());
        }
        for (DynamicMetric dm : newMetrics.values()) {
            memory.claimMemory(dm.memoryUsed());
        }
        this.dynamicMetrics = newMetrics;

        try {
            /* close temp index reader */
            this.flamdexReaderRef.close();
        } catch (IOException e) {
            log.error("Could not close optimized reader");
        }

        /* reopen the original flamdex readers */
        this.flamdexReader = this.originalReader;
        this.flamdexReaderRef = this.originalReaderRef;
        this.originalReader = null;
        this.originalReaderRef = null;

        this.numDocs = this.flamdexReader.getNumDocs();

        /* push the stats back on */
        for (String stat : statsCopy) {
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
    public synchronized void resetGroups() throws ImhotepOutOfMemoryException {
        resetOptimizedReaders();
        resetGroupsTo(1);
    }

    @Override
    public synchronized long[] getGroupStats(int stat) {
        if (groupStats.isDirty(stat)) {
            updateGroupStatsAllDocs(statLookup.get(stat),
                                    groupStats.get(stat),
                                    docIdToGroup,
                                    docGroupBuffer,
                                    docIdBuf,
                                    valBuf);
            groupStats.validate(stat);
        }
        return groupStats.get(stat);
    }

    private static void updateGroupStatsAllDocs(IntValueLookup statLookup,
                                                long[]         results,
                                                GroupLookup    docIdToGroup,
                                                int[]          docGrpBuffer,
                                                int[]          docIdBuf,
                                                long[]         valBuf) {
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

    static void updateGroupStatsDocIdBuf(IntValueLookup statLookup,
                                         long[] results,
                                         int[] docGrpBuffer,
                                         int[] docIdBuf,
                                         long[] valBuf,
                                         int n) {
        /* This is a hacky optimization, but probably worthwhile, since Count is
         * such a common stat. Rather than have Count fill up an array with ones
         * and then add each in turn to our results, we just increment the
         * results directly. */
        if (statLookup instanceof com.indeed.imhotep.metrics.Count) {
            for (int i = 0; i < n; i++) {
                results[docGrpBuffer[i]] += 1;
            }
        }
        else {
            statLookup.lookup(docIdBuf, valBuf, n);
            for (int i = 0; i < n; i++) {
                results[docGrpBuffer[i]] += valBuf[i];
            }
        }
    }

    @Override
    protected void tryClose() {
        /* clean up the optimization log */
        if (this.optimizationLog.exists()) {
            this.optimizationLog.delete();
        }
        try {
            /* close temp readers, if there are any */
            if (this.originalReader != null) {
                this.flamdexReaderRef.close();
                this.flamdexReader = this.originalReader;
                this.flamdexReaderRef = this.originalReaderRef;
            }
        } catch (IOException e) {
            log.error("Could not close optimized reader");
        }

        super.tryClose();
    }
}

package com.indeed.imhotep.local;

import com.indeed.flamdex.api.IntValueLookup;
import com.indeed.flamdex.datastruct.FastBitSet;
import com.indeed.imhotep.BitTree;
import com.indeed.imhotep.GroupRemapRule;
import com.indeed.util.core.threads.ThreadSafeBitSet;
import org.apache.log4j.Logger;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.List;

/**
 * @author jplaisance
 */
public final class MultiCache implements Closeable {
    private static final Logger log = Logger.getLogger(MultiCache.class);
    private static final int MAX_GROUP_NUM = 1 << 28;
    private static final int BLOCK_COPY_SIZE = 8192;

    private long nativeShardDataPtr;
    private final int numDocsInShard;
    private final int numStats;
    private final List<MultiCacheIntValueLookup> nativeMetricLookups;
    private final MultiCacheGroupLookup nativeGroupLookup;

    private final ImhotepLocalSession session;
    private int closedLookupCount = 0;

    private static final int CHUNK_SIZE = 4096;

    public MultiCache(ImhotepLocalSession session,
                      int numDocsInShard,
                      MultiCacheConfig config,
                      StatLookup stats,
                      GroupLookup groupLookup) {
        MultiCacheConfig.StatsOrderingInfo[] ordering = config.getOrdering();
        this.session = session;
        this.numDocsInShard = numDocsInShard;
        this.numStats = ordering.length;

        this.nativeShardDataPtr = buildCache(ordering, this.numStats);

        /* create the group lookup and populate the groups */
        this.nativeGroupLookup = new MultiCacheGroupLookup();
        copyGroups(groupLookup, numDocsInShard);

        /* create the metric IntValueLookups, and populate the metrics in the multicache */
        this.nativeMetricLookups = new ArrayList<MultiCacheIntValueLookup>(this.numStats);
        for (int col = 0; col < ordering.length; col++) {
            final MultiCacheConfig.StatsOrderingInfo orderInfo = ordering[col];
            final MultiCacheIntValueLookup metricLookup;
            metricLookup = new MultiCacheIntValueLookup(col, orderInfo.min, orderInfo.max);
            this.nativeMetricLookups.add(metricLookup);
        }
        int row;
        for (row = 0; row + CHUNK_SIZE < numDocsInShard; row = row + CHUNK_SIZE) {
            for (int col = 0; col < ordering.length; col++) {
                final MultiCacheConfig.StatsOrderingInfo orderInfo = ordering[col];
                /* copy data into multicache */
                copyValues(stats.get(orderInfo.originalOrder), row, CHUNK_SIZE, col);
            }
        }
        int remaining = numDocsInShard - row;
        if (remaining > 0) {
            for (int col = 0; col < ordering.length; col++) {
                final MultiCacheConfig.StatsOrderingInfo orderInfo = ordering[col];
                /* copy data into multicache */
                copyValues(stats.get(orderInfo.originalOrder), row, remaining, col);
            }
        }
    }

    private long buildCache(MultiCacheConfig.StatsOrderingInfo[] ordering, int count) {
        final long[] mins = new long[count];
        final long[] maxes = new long[count];
        final int[] sizesInBytes = new int[count];
        final int[] vectorNums = new int[count];
        final int[] offsetsInVectors = new int[count];
        final byte[] originalOrder = new byte[count];

        for (int i = 0; i < ordering.length; i++) {
            final MultiCacheConfig.StatsOrderingInfo orderInfo = ordering[i];
            mins[i] = orderInfo.min;
            maxes[i] = orderInfo.max;
            sizesInBytes[i] = orderInfo.sizeInBytes;
            vectorNums[i] = orderInfo.vectorNum;
            offsetsInVectors[i] = orderInfo.offsetInVector;
            originalOrder[orderInfo.originalOrder] = (byte)i;
        }
        return nativeBuildMultiCache(numDocsInShard,
                                     mins,
                                     maxes,
                                     sizesInBytes,
                                     vectorNums,
                                     offsetsInVectors,
                                     originalOrder,
                                     this.numStats);
    }

    final int[] copyValuesIdBuffer = new int[CHUNK_SIZE];
    final long[] copyValuesBuffer = new long[CHUNK_SIZE];

    private void copyValues(IntValueLookup original, int start, int count, int metricId) {
        for (int i = 0; i < count; i++) {
            copyValuesIdBuffer[i] = start + i;
        }
        original.lookup(copyValuesIdBuffer, copyValuesBuffer, count);
        nativePackMetricDataInRange(this.nativeShardDataPtr, metricId, start, count, copyValuesBuffer);
    }

    private void copyGroups(GroupLookup original, int numDocsInShard) {
        final int[] groupBuffer = new int[BLOCK_COPY_SIZE];

        for (int start = 0; start < numDocsInShard; start += BLOCK_COPY_SIZE) {
            final int end = Math.min(numDocsInShard, start + BLOCK_COPY_SIZE);
            final int n = end - start;
            original.fillDocGrpBufferSequential(start, groupBuffer, n);
            nativeSetGroupsInRange(this.nativeShardDataPtr, start, n, groupBuffer);
        }
        this.nativeGroupLookup.numGroups = original.getNumGroups();
    }

    public IntValueLookup getIntValueLookup(int statIndex) {
        return this.nativeMetricLookups.get(statIndex);
    }

    public GroupLookup getGroupLookup() {
        return this.nativeGroupLookup;
    }


    private void childLookupClosed() {
        closedLookupCount++;
        if (closedLookupCount == this.numStats) {
            this.close();
        }
    }

    @Override
    public void close() {
//        if (this.session.docIdToGroup == this.nativeGroupLookup) {
//            /*
//             * session is still using the group lookup
//             * free the native memory in the finalizer
//             */
//            return;
//        }
        nativeDestroyMultiCache(this.nativeShardDataPtr);
        this.nativeShardDataPtr = 0;
    }

//    @Override
//    protected void finalize() {
//        if (this.nativeShardDataPtr != 0) {
//            nativeDestroyMultiCache(this.nativeShardDataPtr);
//        }
//    }

    private native void nativeDestroyMultiCache(long nativeShardDataPtr);

    private native long nativeBuildMultiCache(int numDocsInShard,
                                              long[] mins,
                                              long[] maxes,
                                              int[] sizesInBytes,
                                              int[] vectorNums,
                                              int[] offsetsInVectors,
                                              byte[] originalOrder,
                                              int numStats);

    private static native void nativePackMetricDataInRange(long nativeShardDataPtr,
                                                           int metricId,
                                                           int start,
                                                           int n,
                                                           long[] valBuffer);

    private native void nativeSetGroupsInRange(long nativeShardDataPtr,
                                               int start,
                                               int count,
                                               int[] groupsBuffer);

    public long getNativeAddress() {
        return this.nativeShardDataPtr;
    }

    private final class MultiCacheIntValueLookup implements IntValueLookup {
        private final int index;
        private final long min;
        private final long max;
        private boolean closed = false;

        private MultiCacheIntValueLookup(int index, long min, long max) {
            this.index = index;
            this.min = min;
            this.max = max;
        }

        public long nativeShardDataPtr() { return MultiCache.this.nativeShardDataPtr; }

        @Override
        public long getMin() {
            return this.min;
        }

        @Override
        public long getMax() {
            return this.max;
        }

        @Override
        public void lookup(int[] docIds, long[] values, int n) {
            nativeMetricLookup(MultiCache.this.nativeShardDataPtr, this.index, docIds, values, n);
        }

        @Override
        public long memoryUsed() {
            return 0;
        }

        @Override
        public void close() {
            if (closed) {
                log.error("MultiCacheIntValueLookup closed twice");
                return;
            }
            closed = true;
            MultiCache.this.childLookupClosed();
        }

        private native void nativeMetricLookup(long nativeShardDataPtr,
                                               int index,
                                               int[] docIds,
                                               long[] values,
                                               int n);
    }

    public final class MultiCacheGroupLookup extends GroupLookup {
        /* should be as large as the buffer passed into nextGroupCallback() */
        private final int[] groups_buffer = new int[ImhotepLocalSession.BUFFER_SIZE];
        private final int[] remap_buffer = new int[ImhotepLocalSession.BUFFER_SIZE];

        @Override
        public void nextGroupCallback(int n, long[][] termGrpStats, BitTree groupsSeen) {
            /* collect group ids for docs */
            nativeFillGroupsBuffer(MultiCache.this.nativeShardDataPtr,
                                   MultiCache.this.session.docIdBuf,
                                   this.groups_buffer,
                                   n);

            int rewriteHead = 0;
            // remap groups and filter out useless docids (ones with group = 0),
            // keep track of groups that were found
            for (int i = 0; i < n; i++) {
                final int group = this.groups_buffer[i];
                if (group == 0) {
                    continue;
                }

                final int docId = MultiCache.this.session.docIdBuf[i];

                MultiCache.this.session.docGroupBuffer[rewriteHead] = group;
                MultiCache.this.session.docIdBuf[rewriteHead] = docId;
                rewriteHead++;
            }
            groupsSeen.set(MultiCache.this.session.docGroupBuffer, rewriteHead);

            if (rewriteHead > 0) {
                for (int statIndex = 0; statIndex < MultiCache.this.session.numStats; statIndex++) {
                    ImhotepLocalSession.updateGroupStatsDocIdBuf(MultiCache.this.session.statLookup.get(statIndex),
                                                                 termGrpStats[statIndex],
                                                                 MultiCache.this.session.docGroupBuffer,
                                                                 MultiCache.this.session.docIdBuf,
                                                                 MultiCache.this.session.valBuf,
                                                                 rewriteHead);
                }
            }
        }

        public long nativeShardDataPtr() { return MultiCache.this.nativeShardDataPtr; }

        @Override
        public void applyIntConditionsCallback(int n,
                                        ThreadSafeBitSet docRemapped,
                                        GroupRemapRule[] remapRules,
                                        String intField,
                                        long itrTerm) {
            /* collect group ids for docs */
            nativeFillGroupsBuffer(MultiCache.this.nativeShardDataPtr,
                                   MultiCache.this.session.docIdBuf,
                                   this.groups_buffer,
                                   n);

            for (int i = 0; i < n; i++) {
                final int docId = MultiCache.this.session.docIdBuf[i];
                if (docRemapped.get(docId)) {
                    continue;
                }

                final int group = this.groups_buffer[i];
                if (remapRules[group] == null) {
                    continue;
                }

                if (ImhotepLocalSession.checkIntCondition(remapRules[group].condition,
                                                          intField,
                                                          itrTerm)) {
                    continue;
                }

                this.remap_buffer[i] = remapRules[group].positiveGroup;
                docRemapped.set(docId);
            }
            /* write updated groups back to the native table/lookup */
            nativeUpdateGroups(MultiCache.this.nativeShardDataPtr,
                               MultiCache.this.session.docIdBuf,
                               this.remap_buffer,
                               n);
        }

        @Override
        public void applyStringConditionsCallback(int n,
                                           ThreadSafeBitSet docRemapped,
                                           GroupRemapRule[] remapRules,
                                           String stringField,
                                           String itrTerm) {
            /* collect group ids for docs */
            nativeFillGroupsBuffer(MultiCache.this.nativeShardDataPtr,
                                   MultiCache.this.session.docIdBuf,
                                   this.groups_buffer,
                                   n);

            for (int i = 0; i < n; i++) {
                final int docId = session.docIdBuf[i];
                if (docRemapped.get(docId)) {
                    continue;
                }

                final int group = this.groups_buffer[i];
                if (remapRules[group] == null) {
                    continue;
                }

                if (ImhotepLocalSession.checkStringCondition(remapRules[group].condition,
                                                             stringField,
                                                             itrTerm)) {
                    continue;
                }
                this.remap_buffer[i] = remapRules[group].positiveGroup;
                docRemapped.set(docId);
            }
            /* write updated groups back to the native table/lookup */
            nativeUpdateGroups(MultiCache.this.nativeShardDataPtr,
                               MultiCache.this.session.docIdBuf,
                               this.remap_buffer,
                               n);
        }

        @Override
        public int get(int doc) {
            return nativeGetGroup(MultiCache.this.nativeShardDataPtr, doc);
        }

        @Override
        public void set(int doc, int group) {
            nativeSetGroupForDoc(MultiCache.this.nativeShardDataPtr, doc, group);
        }

        @Override
        public void batchSet(int[] docIdBuf, int[] docGrpBuffer, int n) {
            nativeUpdateGroups(MultiCache.this.nativeShardDataPtr, docIdBuf, docGrpBuffer, n);
        }

        @Override
        public void fill(int group) {
            nativeSetAllGroups(MultiCache.this.nativeShardDataPtr, group);
        }

        @Override
        public void copyInto(GroupLookup other) {
            if (this.size() != other.size()) {
                throw new IllegalArgumentException("size != other.size: size=" + this.size()
                        + ", other.size=" + other.size());
            }

            int start = 0;
            while (start < MultiCache.this.numDocsInShard) {
                final int count =
                        Math.min(ImhotepLocalSession.BUFFER_SIZE,
                                 MultiCache.this.numDocsInShard - start);

                /* load groups into a buffer */
                nativeUpdateGroupsSequential(MultiCache.this.nativeShardDataPtr,
                                           start,
                                           count,
                                           this.groups_buffer);

                /* copy into other */
                for (int i = 0; i < count; ++i) {
                    other.set(i + start, this.groups_buffer[i]);
                }

                start += count;
            }
            other.numGroups = this.numGroups;
        }

        @Override
        public int size() {
            return MultiCache.this.numDocsInShard;
        }

        @Override
        public int maxGroup() {
            return MAX_GROUP_NUM;
        }

        @Override
        public long memoryUsed() {
            return this.groups_buffer.length * 4L + this.remap_buffer.length * 4L;
        }

        @Override
        public void fillDocGrpBuffer(int[] docIdBuf, int[] docGrpBuffer, int n) {
            nativeFillGroupsBuffer(MultiCache.this.nativeShardDataPtr, docIdBuf, docGrpBuffer, n);
        }

        @Override
        public void fillDocGrpBufferSequential(int start, int[] docGrpBuffer, int n) {
            nativeUpdateGroupsSequential(MultiCache.this.nativeShardDataPtr, start, n, docGrpBuffer);
        }

        @Override
        public void bitSetRegroup(FastBitSet bitSet, int targetGroup, int negativeGroup, int positiveGroup) {
            nativeBitSetRegroup(MultiCache.this.nativeShardDataPtr,
                                bitSet.getBackingArray(),
                                targetGroup,
                                negativeGroup,
                                positiveGroup);
        }

        @Override
        public ImhotepLocalSession getSession() {
            return MultiCache.this.session;
        }

        @Override
        protected void recalculateNumGroups() {
            this.numGroups = nativeRecalculateNumGroups(MultiCache.this.nativeShardDataPtr);
            return;
        }

        private native void nativeFillGroupsBuffer(long nativeShardDataPtr,
                                                   int[] docIdBuf,
                                                   int[] groups_buffer,
                                                   int n);

        private native void nativeUpdateGroups(long nativeShardDataPtr,
                                               int[] docIdBuf,
                                               int[] groups,
                                               int n);

        private native void nativeUpdateGroupsSequential(long nativeShardDataPtr,
                                                         int start,
                                                         int count,
                                                         int[] grpBuffer);

        private native int nativeGetGroup(long nativeShardDataPtr, int doc);

        private native void nativeSetGroupForDoc(long nativeShardDataPtr, int doc, int group);

        private native void nativeSetAllGroups(long nativeShardDataPtr, int group);

        private native void nativeBitSetRegroup(long nativeShardDataPtr,
                                                long[] bitset,
                                                int targetGroup,
                                                int negativeGroup,
                                                int positiveGroup);

        private native int nativeRecalculateNumGroups(long nativeShardDataPtr);
    }
}

package com.indeed.imhotep.local;

import com.indeed.flamdex.api.IntValueLookup;
import com.indeed.flamdex.datastruct.FastBitSet;
import com.indeed.imhotep.BitTree;
import com.indeed.imhotep.GroupRemapRule;
import com.indeed.util.core.threads.ThreadSafeBitSet;
import org.apache.log4j.Logger;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author jplaisance
 */
public final class MultiCache implements Closeable {
    private static final Logger log = Logger.getLogger(MultiCache.class);
    private static final int BLOCK_COPY_SIZE = 8192;

    private final long flamdexDoclistAddress;
    private long nativeShardDataPtr;
    private final int numDocsInShard;
    private final int numStats;
    private final List<MultiCacheIntValueLookup> nativeMetricLookups;
    private final MultiCacheGroupLookup nativeGroupLookup;

    private final ImhotepLocalSession session;


    public static class StatsOrderingInfo {
        List<IntValueLookup> reorderedMetrics;
        int[] mins;
        int[] maxes;
    }


    public MultiCache(ImhotepLocalSession session,
                      long flamdexDoclistAddress,
                      int numDocsInShard,
                      List<IntValueLookup> metrics,
                      NativeMetricsOrdering ordering) {
        this.session = session;
        this.flamdexDoclistAddress = flamdexDoclistAddress;
        this.numDocsInShard = numDocsInShard;
        this.numStats = metrics.size();

        this.nativeGroupLookup = new MultiCacheGroupLookup();
        this.nativeMetricLookups = new ArrayList<MultiCacheIntValueLookup>(this.numStats);

        final StatsOrderingInfo orderInfo = ordering.getOrder(metrics);
        this.nativeShardDataPtr = nativeBuildMultiCache(nativeShardDataPtr,
                                                        numDocsInShard,
                                                        orderInfo.mins,
                                                        orderInfo.maxes,
                                                        this.numStats);

        for (int i = 0; i < numStats; i++) {
            nativeMetricLookups.add(new MultiCacheIntValueLookup(i,
                                                                 orderInfo.mins[i],
                                                                 orderInfo.maxes[i]));

            /* copy data into multicache */
            IntValueLookup metric = orderInfo.reorderedMetrics.get(i);
            copyValues(metric, numDocsInShard, i);
        }
    }

    private final void copyValues(IntValueLookup original, int numDocsInShard, int metricId) {
        final int[] idBuffer = new int[BLOCK_COPY_SIZE];
        final long[] valBuffer = new long[BLOCK_COPY_SIZE];

        for (int start = 0; start < numDocsInShard; start += BLOCK_COPY_SIZE) {
            final int end = Math.min(numDocsInShard, start + BLOCK_COPY_SIZE);
            final int n = end - start;
            for (int i = 0; i < n; i++) {
                idBuffer[i] = start + i;
            }
            original.lookup(idBuffer, valBuffer, n);
            nativePackMetricDataInRange(nativeShardDataPtr, metricId, start, n, valBuffer);
        }
    }

    public IntValueLookup getIntValueLookup(int statIndex) {
        return this.nativeMetricLookups.get(statIndex);
    }

    public GroupLookup getGroupLookup() {
        return this.nativeGroupLookup;
    }

    @Override
    public void close() throws IOException {

    }

    private native long nativeBuildMultiCache(long nativeShardDataPtr,
                                              int numDocsInShard,
                                              int[] mins,
                                              int[] maxes,
                                              int numStats);

    private static native void nativePackMetricDataInRange(long nativeShardDataPtr,
                                                           int metricId,
                                                           int start,
                                                           int n,
                                                           long[] valBuffer);

    private final class MultiCacheIntValueLookup implements IntValueLookup {
        private final int index;
        private final long min;
        private final long max;

        private MultiCacheIntValueLookup(int index, long min, long max) {
            this.index = index;
            this.min = min;
            this.max = max;
        }

        @Override
        public long getMin() {
            return min;
        }

        @Override
        public long getMax() {
            return max;
        }

        @Override
        public void lookup(int[] docIds, long[] values, int n) {
            nativeMetricLookup(nativeShardDataPtr, index, docIds, values, n);
        }

        @Override
        public long memoryUsed() {
            return 0;
        }

        @Override
        public void close() {

        }

        private native void nativeMetricLookup(long nativeShardDataPtr,
                                               int index,
                                               int[] docIds,
                                               long[] values,
                                               int n);
    }


    private final class MultiCacheGroupLookup extends GroupLookup {
        /* should be as large as the buffer passed into nextGroupCallback() */
        private final int[] groups_buffer = new int[ImhotepLocalSession.BUFFER_SIZE];
        private final int[] remap_buffer = new int[ImhotepLocalSession.BUFFER_SIZE];

        @Override
        void nextGroupCallback(int n, long[][] termGrpStats, BitTree groupsSeen) {
            /* collect group ids for docs */
            nativeFillGroupsBuffer(nativeShardDataPtr, this.groups_buffer, session.docIdBuf, n);

            int rewriteHead = 0;
            // remap groups and filter out useless docids (ones with group = 0), keep track of groups that were found
            for (int i = 0; i < n; i++) {
                final int group = groups_buffer[i];
                if (group == 0)
                    continue;

                final int docId = session.docIdBuf[i];

                session.docGroupBuffer[rewriteHead] = group;
                session.docIdBuf[rewriteHead] = docId;
                rewriteHead++;
            }
            groupsSeen.set(session.docGroupBuffer, rewriteHead);

            if (rewriteHead > 0) {
                for (int statIndex = 0; statIndex < session.numStats; statIndex++) {
                    ImhotepLocalSession.updateGroupStatsDocIdBuf(session.statLookup[statIndex],
                                                                 termGrpStats[statIndex],
                                                                 session.docGroupBuffer,
                                                                 session.docIdBuf,
                                                                 session.valBuf,
                                                                 rewriteHead);
                }
            }
        }

        @Override
        void applyIntConditionsCallback(int n,
                                        ThreadSafeBitSet docRemapped,
                                        GroupRemapRule[] remapRules,
                                        String intField,
                                        long itrTerm) {
            /* collect group ids for docs */
            nativeFillGroupsBuffer(nativeShardDataPtr, this.groups_buffer, session.docIdBuf, n);

            for (int i = 0; i < n; i++) {
                final int docId = session.docIdBuf[i];
                if (docRemapped.get(docId))
                    continue;

                final int group = groups_buffer[i];
                if (remapRules[group] == null)
                    continue;

                if (ImhotepLocalSession.checkIntCondition(remapRules[group].condition,
                                                          intField,
                                                          itrTerm))
                    continue;

                remap_buffer[i] = remapRules[group].positiveGroup;
                docRemapped.set(docId);
            }
            /* write updated groups back to the native table/lookup */
            nativeUpdateGroups(nativeShardDataPtr, this.remap_buffer, session.docIdBuf, n);
        }

        @Override
        void applyStringConditionsCallback(int n,
                                           ThreadSafeBitSet docRemapped,
                                           GroupRemapRule[] remapRules,
                                           String stringField,
                                           String itrTerm) {
            /* collect group ids for docs */
            nativeFillGroupsBuffer(nativeShardDataPtr, this.groups_buffer, session.docIdBuf, n);

            for (int i = 0; i < n; i++) {
                final int docId = session.docIdBuf[i];
                if (docRemapped.get(docId))
                    continue;

                final int group = groups_buffer[i];
                if (remapRules[group] == null)
                    continue;

                if (ImhotepLocalSession.checkStringCondition(remapRules[group].condition,
                                                             stringField,
                                                             itrTerm)) {
                    continue;
                    remap_buffer[i] = remapRules[group].positiveGroup;
                }
                docRemapped.set(docId);
            }
            /* write updated groups back to the native table/lookup */
            nativeUpdateGroups(nativeShardDataPtr, this.remap_buffer, session.docIdBuf, n);
        }

        @Override
        int get(int doc) {
            return nativeGetGroup(nativeShardDataPtr, doc);
        }

        @Override
        void set(int doc, int group) {
            nativeSetGroupForDoc(nativeShardDataPtr, doc, group);
        }

        @Override
        void batchSet(int[] docIdBuf, int[] docGrpBuffer, int n) {
            nativeUpdateGroups(nativeShardDataPtr, docGrpBuffer, docIdBuf, n);
        }

        @Override
        void fill(int group) {
            nativeSetAllGroups(nativeShardDataPtr, group);
        }

        @Override
        void copyInto(GroupLookup other) {
            if (this.size() != other.size()) {
                throw new IllegalArgumentException("size != other.size: size="
                                                           + this.size()
                                                           + ", other.size="
                                                           + other.size());
            }

            int start = 0;
            while (start < numDocsInShard) {
                int count = Math.min(ImhotepLocalSession.BUFFER_SIZE, numDocsInShard - start);

                /* load groups into a buffer */
                nativeFillGroupsSequential(nativeShardDataPtr, start, count, this.groups_buffer);

                /* copy into other */
                for (int i = 0; i < count; ++i) {
                    other.set(i + start, this.groups_buffer[i]);
                }

                start += count;
            }
            other.numGroups = this.numGroups;
        }

        @Override
        int size() {
            return MultiCache.this.numDocsInShard;
        }

        @Override
        int maxGroup() {
            return 0;  //TODO
        }

        @Override
        long memoryUsed() {
            return this.groups_buffer.length * 4L
                    + this.remap_buffer.length * 4L;
        }

        @Override
        void fillDocGrpBuffer(int[] docIdBuf, int[] docGrpBuffer, int n) {
            nativeFillGroupsBuffer(nativeShardDataPtr, docGrpBuffer, docIdBuf, n);
        }

        @Override
        void fillDocGrpBufferSequential(int start, int[] docGrpBuffer, int n) {
            nativeFillGroupsSequential(nativeShardDataPtr, start, n, docGrpBuffer);
        }

        @Override
        void bitSetRegroup(FastBitSet bitSet,
                           int targetGroup,
                           int negativeGroup,
                           int positiveGroup) {
            nativeBitSetRegroup(nativeShardDataPtr,
                                bitSet.bits,
                                targetGroup,
                                negativeGroup,
                                positiveGroup);
        }

        @Override
        ImhotepLocalSession getSession() {
            return MultiCache.this.session;
        }

        @Override
        void recalculateNumGroups() {
// TODO
        }

        private native void nativeFillGroupsBuffer(long nativeShardDataPtr,
                                                   int[] groups_buffer,
                                                   int[] docIdBuf,
                                                   int n);

        private native void nativeUpdateGroups(long nativeShardDataPtr,
                                               int[] remap_buffer,
                                               int[] docIdBuf,
                                               int n);

        private native void nativeFillGroupsSequential(long nativeShardDataPtr,
                                                       int start,
                                                       int count,
                                                       int[] grpBuffer);

        private native void nativeUpdateGroupsSequential(long nativeShardDataPtr,
                                                         int[] newGroups,
                                                         int start,
                                                         int n);

    }
}

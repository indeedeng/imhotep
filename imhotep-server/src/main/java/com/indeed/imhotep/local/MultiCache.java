package com.indeed.imhotep.local;

import com.indeed.flamdex.api.IntValueLookup;
import com.indeed.flamdex.datastruct.FastBitSet;
import com.indeed.imhotep.BitTree;
import com.indeed.imhotep.GroupRemapRule;
import com.indeed.util.core.threads.ThreadSafeBitSet;
import org.apache.log4j.Logger;

import java.io.Closeable;
import java.io.IOException;

/**
 * @author jplaisance
 */
public final class MultiCache implements Closeable {
    private static final Logger log = Logger.getLogger(MultiCache.class);

    private final long address;

    public MultiCache(long address) {
        this.address = address;
    }

    public IntValueLookup getIntValueLookup(int statIndex) {
        //TODO
        return null;
    }

    public GroupLookup getGroupLookup() {
        //TODO
        return null;
    }

    @Override
    public void close() throws IOException {

    }

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

        }

        @Override
        public long memoryUsed() {
            return 0;
        }

        @Override
        public void close() {

        }
    }

    private final class MultiCacheGroupLookup extends GroupLookup {

        @Override
        void nextGroupCallback(int n, long[][] termGrpStats, BitTree groupsSeen) {

        }

        @Override
        void applyIntConditionsCallback(int n, ThreadSafeBitSet docRemapped, GroupRemapRule[] remapRules, String intField, long itrTerm) {

        }

        @Override
        void applyStringConditionsCallback(int n, ThreadSafeBitSet docRemapped, GroupRemapRule[] remapRules, String stringField, String itrTerm) {

        }

        @Override
        int get(int doc) {
            return 0;
        }

        @Override
        void set(int doc, int group) {

        }

        @Override
        void batchSet(int[] docIdBuf, int[] docGrpBuffer, int n) {

        }

        @Override
        void fill(int group) {

        }

        @Override
        void copyInto(GroupLookup other) {

        }

        @Override
        int size() {
            return 0;
        }

        @Override
        int maxGroup() {
            return 0;
        }

        @Override
        long memoryUsed() {
            return 0;
        }

        @Override
        void fillDocGrpBuffer(int[] docIdBuf, int[] docGrpBuffer, int n) {

        }

        @Override
        void fillDocGrpBufferSequential(int start, int[] docGrpBuffer, int n) {

        }

        @Override
        void bitSetRegroup(FastBitSet bitSet, int targetGroup, int negativeGroup, int positiveGroup) {

        }

        @Override
        ImhotepLocalSession getSession() {
            return null;
        }

        @Override
        void recalculateNumGroups() {

        }
    }
}

package com.indeed.imhotep.local;

import java.util.Arrays;

import com.indeed.util.core.Pair;
import com.indeed.util.core.io.Closeables2;
import com.indeed.util.core.reference.SharedReference;
import com.indeed.flamdex.api.FlamdexReader;
import com.indeed.flamdex.api.IntTermDocIterator;
import com.indeed.flamdex.api.StringTermDocIterator;
import com.indeed.flamdex.datastruct.FastBitSet;
import com.indeed.imhotep.api.FTGSIterator;

class FlamdexFTGSIterator implements FTGSIterator {
    /**
     * 
     */
    private final ImhotepLocalSession session;
    private final String[] intFields;
    private final String[] stringFields;
    private int intFieldPtr = 0;
    private int stringFieldPtr = 0;

    private String currentField;
    private boolean currentFieldIsIntType;

    private IntTermDocIterator intTermDocIterator;
    protected StringTermDocIterator stringTermDocIterator;

    private int groupPointer;
    private int groupsSeenCount;
    private final int[] groupsSeen;

    private final long[] termGrpCounts;
    private final long[][] termGrpStats;

    private boolean resetGroupStats = false;

    private FastBitSet fieldZeroDocBitset;
    private int termIndex;

    private SharedReference<FlamdexReader> flamdexReader;

    long intTermsTime = 0;
    long stringTermsTime = 0;
    long docsTime = 0;
    long lookupsTime = 0;
    long timingErrorTime = 0;

    public FlamdexFTGSIterator(ImhotepLocalSession imhotepLocalSession, SharedReference<FlamdexReader> flamdexReader, String[] intFields, String[] stringFields) {
        this.session = imhotepLocalSession;
        this.flamdexReader = flamdexReader;
        this.intFields = intFields;
        this.stringFields = stringFields;
        this.groupsSeen = new int[session.docIdToGroup.getNumGroups()];
        this.termGrpCounts = new long[session.docIdToGroup.getNumGroups()];
        this.termGrpStats = new long[session.numStats][session.docIdToGroup.getNumGroups()];
    }

    @Override
    public final boolean nextField() {
        // todo: reset/cleanup term iterators etc that are in progress
        synchronized (session) {
            if (intFieldPtr < intFields.length) {
                currentField = intFields[intFieldPtr++];
                currentFieldIsIntType = true;
                if (intTermDocIterator != null) Closeables2.closeQuietly(intTermDocIterator, ImhotepLocalSession.log);
                intTermDocIterator = flamdexReader.get().getIntTermDocIterator(currentField);
                if (session.fieldZeroDocBitsets != null) {
                    fieldZeroDocBitset = session.fieldZeroDocBitsets.get(Pair.of(currentField, currentFieldIsIntType));
                }
                termIndex = 0;
                return true;
            }
            if (stringFieldPtr < stringFields.length) {
                currentField = stringFields[stringFieldPtr++];
                currentFieldIsIntType = false;
                if (stringTermDocIterator != null) Closeables2.closeQuietly(stringTermDocIterator, ImhotepLocalSession.log);
                stringTermDocIterator = flamdexReader.get().getStringTermDocIterator(currentField);
                if (session.fieldZeroDocBitsets != null) {
                    fieldZeroDocBitset = session.fieldZeroDocBitsets.get(Pair.of(currentField, currentFieldIsIntType));
                }
                termIndex = 0;
                return true;
            }
            currentField = null;
            close();
            if (ImhotepLocalSession.logTiming) {
                ImhotepLocalSession.log.info("intTermsTime: "+intTermsTime/1000000d+" ms, stringTermsTime: "+stringTermsTime/1000000d+" ms, docsTime: "+docsTime/1000000d+" ms, lookupsTime: "+lookupsTime/1000000d+" ms, timingErrorTime: "+timingErrorTime/1000000d+" ms");
            }
            return false;
        }
    }

    @Override
    public final void close() {
        synchronized (session) {
            if (intTermDocIterator != null) {
                Closeables2.closeQuietly(intTermDocIterator, ImhotepLocalSession.log);
                intTermDocIterator = null;
            }
            if (stringTermDocIterator != null) {
                Closeables2.closeQuietly(stringTermDocIterator, ImhotepLocalSession.log);
                stringTermDocIterator = null;
            }
            if (flamdexReader != null) {
                Closeables2.closeQuietly(flamdexReader, ImhotepLocalSession.log);
                flamdexReader = null;
            }
        }
    }

    @Override
    public final String fieldName() {
        return currentField;
    }

    @Override
    public final boolean fieldIsIntType() {
        return currentFieldIsIntType;
    }

    @Override
    public final boolean nextTerm() {
        if (currentField == null) return false;
        resetGroupStats = true;
        if (currentFieldIsIntType) {
            if (ImhotepLocalSession.logTiming) intTermsTime -= System.nanoTime();
            final boolean ret = intTermDocIterator.nextTerm();
            if (ImhotepLocalSession.logTiming) intTermsTime += System.nanoTime();
            return ret;
        } else {
            if (ImhotepLocalSession.logTiming) stringTermsTime -= System.nanoTime();
            final boolean ret = stringTermDocIterator.nextTerm();
            if (ImhotepLocalSession.logTiming) stringTermsTime += System.nanoTime();
            return ret;
        }
    }

    @Override
    public final long termDocFreq() {
        return currentFieldIsIntType ? intTermDocIterator.docFreq() : stringTermDocIterator.docFreq();
    }

    @Override
    public final long termIntVal() {
        return intTermDocIterator.term();
    }

    @Override
    public final String termStringVal() {
        return stringTermDocIterator.term();
    }

    @Override
    public final boolean nextGroup() {
        if (!resetGroupStats) {
            if (groupPointer >= groupsSeenCount) return false;
            groupPointer++;
            return groupPointer < groupsSeenCount;
        }

        // clear out ram from previous iterations if necessary
        ImhotepLocalSession.clear(termGrpCounts, groupsSeen, groupsSeenCount);
        for (final long[] x : termGrpStats) ImhotepLocalSession.clear(x, groupsSeen, groupsSeenCount);
        groupsSeenCount = 0;

        if (fieldZeroDocBitset != null) {
            if (termIndex == fieldZeroDocBitset.size()) expandFieldNonZeroDocBitset();
            final boolean skip = fieldZeroDocBitset.get(termIndex);
            termIndex++;
            if (skip) return false;
        }

        // this is the critical loop of all of imhotep, making this loop faster is very good....

        synchronized (session) {
            while (true) {
                if (ImhotepLocalSession.logTiming) docsTime -= System.nanoTime();
                final int n = (currentFieldIsIntType?intTermDocIterator:stringTermDocIterator).fillDocIdBuffer(session.docIdBuf);
                if (ImhotepLocalSession.logTiming) {
                    docsTime += System.nanoTime();
                    lookupsTime -= System.nanoTime();
                }
                groupsSeenCount = session.docIdToGroup.nextGroupCallback(n, termGrpCounts, termGrpStats, groupsSeen, groupsSeenCount);
                if (ImhotepLocalSession.logTiming) {
                    lookupsTime += System.nanoTime();
                    timingErrorTime -= System.nanoTime();
                    timingErrorTime += System.nanoTime();
                }
                if (n < ImhotepLocalSession.BUFFER_SIZE) break;
            }
        }

        Arrays.sort(groupsSeen, 0, groupsSeenCount);
        if (fieldZeroDocBitset != null && groupsSeenCount == 0) {
            fieldZeroDocBitset.set(termIndex - 1);
        }

        groupPointer = 0;
        resetGroupStats = false;
        return groupsSeenCount > 0;
    }

    private void expandFieldNonZeroDocBitset() {
        synchronized (session) {
            if (fieldZeroDocBitset == null) return;
            if(session.memory.claimMemory(FastBitSet.calculateMemoryUsage(fieldZeroDocBitset.size() * 2))) {
                final FastBitSet tmpBitset = new FastBitSet(fieldZeroDocBitset.size() * 2);
                tmpBitset.or(fieldZeroDocBitset);
                final long oldSize = fieldZeroDocBitset.memoryUsage();
                fieldZeroDocBitset = tmpBitset;
                session.fieldZeroDocBitsets.put(Pair.of(currentField, currentFieldIsIntType), fieldZeroDocBitset);
                session.memory.releaseMemory(oldSize);
            } else {
                ImhotepLocalSession.log.warn("Insufficient expansion memory, disabling ftgs zero group bitset optimization");
                session.clearZeroDocBitsets();
                fieldZeroDocBitset = null;
                session.fieldZeroDocBitsets = null;
            }
        }
    }

    @Override
    public final int group() {
        return groupsSeen[groupPointer];
    }

    @Override
    public final void groupStats(long[] stats) {
        final int group = group();
        for (int i = 0; i < session.numStats; i++) {
            stats[i] = termGrpStats[i][group];
        }
    }
}
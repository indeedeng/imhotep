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

import com.indeed.flamdex.api.FlamdexReader;
import com.indeed.imhotep.BitTree;
import com.indeed.imhotep.api.FTGSIterator;
import com.indeed.util.core.Pair;
import com.indeed.util.core.reference.SharedReference;
import org.apache.log4j.Logger;

/**
 * @author jplaisance
 */
public abstract class AbstractFlamdexFTGSIterator implements FTGSIterator {
    private static final Logger log = Logger.getLogger(AbstractFlamdexFTGSIterator.class);
    /**
     *
     */
    protected final ImhotepLocalSession session;

    protected final int[] groupsSeen;
    protected final BitTree bitTree;
    protected final long[][] termGrpStats;

    protected boolean currentFieldIsIntType;


    protected SharedReference<FlamdexReader> flamdexReader;
    long intTermsTime = 0;
    long stringTermsTime = 0;
    long docsTime = 0;
    long lookupsTime = 0;
    long timingErrorTime = 0;

    protected String currentField;

    private int groupPointer;
    private int groupsSeenCount;
    protected boolean resetGroupStats = false;
    protected int termIndex;

    public AbstractFlamdexFTGSIterator(ImhotepLocalSession imhotepLocalSession, SharedReference<FlamdexReader> flamdexReader) {
        this.session = imhotepLocalSession;
        this.termGrpStats = new long[session.numStats][session.docIdToGroup.getNumGroups()];
        this.groupsSeen = new int[session.docIdToGroup.getNumGroups()];
        this.bitTree = new BitTree(session.docIdToGroup.getNumGroups());
        this.flamdexReader = flamdexReader;
    }

    @Override
    public abstract boolean nextField();

    @Override
    public abstract void close();

    @Override
    public final String fieldName() {
        return currentField;
    }

    @Override
    public final boolean fieldIsIntType() {
        return currentFieldIsIntType;
    }

    @Override
    public abstract boolean nextTerm();

    @Override
    public abstract long termDocFreq();

    @Override
    public abstract long termIntVal();

    @Override
    public abstract String termStringVal();

    @Override
    public final boolean nextGroup() {
        if (!resetGroupStats) {
            if (groupPointer >= groupsSeenCount) return false;
            groupPointer++;
            return groupPointer < groupsSeenCount;
        }
        return calculateTermGroupStats();
    }

    private boolean calculateTermGroupStats() {
        // clear out ram from previous iterations if necessary
        for (final long[] x : termGrpStats) ImhotepLocalSession.clear(x, groupsSeen, groupsSeenCount);
        groupsSeenCount = 0;

        // this is the critical loop of all of imhotep, making this loop faster is very good....

        synchronized (session) {
            while (true) {
                if (ImhotepLocalSession.logTiming) docsTime -= System.nanoTime();
                final int n = fillDocIdBuffer();
                if (ImhotepLocalSession.logTiming) {
                    docsTime += System.nanoTime();
                    lookupsTime -= System.nanoTime();
                }
                session.docIdToGroup.nextGroupCallback(n, termGrpStats, bitTree);
                if (ImhotepLocalSession.logTiming) {
                    lookupsTime += System.nanoTime();
                    timingErrorTime -= System.nanoTime();
                    timingErrorTime += System.nanoTime();
                }
                if (n < ImhotepLocalSession.BUFFER_SIZE) break;
            }
        }
        groupsSeenCount = bitTree.dump(groupsSeen);

        groupPointer = 0;
        resetGroupStats = false;
        return groupsSeenCount > 0;
    }

    protected abstract int fillDocIdBuffer();

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

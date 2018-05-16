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

import com.indeed.flamdex.MemoryFlamdex;
import com.indeed.flamdex.api.FlamdexReader;
import com.indeed.flamdex.dynamic.DynamicFlamdexReader;
import com.indeed.flamdex.dynamic.SegmentReader;
import com.indeed.flamdex.lucene.LuceneFlamdexReader;
import com.indeed.flamdex.ramses.RamsesFlamdexWrapper;
import com.indeed.flamdex.reader.MockFlamdexReader;
import com.indeed.flamdex.simple.SimpleFlamdexReader;
import com.indeed.imhotep.BitTree;
import com.indeed.imhotep.api.FTGSIterator;
import com.indeed.imhotep.metrics.Count;
import com.indeed.imhotep.service.CachedFlamdexReader;
import com.indeed.imhotep.service.CachedFlamdexReaderReference;
import com.indeed.imhotep.service.InstrumentedFlamdexReader;
import com.indeed.util.core.reference.SharedReference;
import org.apache.log4j.Logger;

import java.io.Closeable;

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
    long termFreqTime = 0;
    long lookupsTime = 0;
    long timingErrorTime = 0;

    protected String currentField;

    private int groupPointer;
    private int groupsSeenCount;
    protected boolean resetGroupStats = false;
    protected int termIndex;
    private final TermGroupStatsCalculator calculator;

    public AbstractFlamdexFTGSIterator(
            final ImhotepLocalSession imhotepLocalSession,
            final SharedReference<FlamdexReader> flamdexReader) {
        this.session = imhotepLocalSession;
        this.termGrpStats = new long[session.numStats][session.docIdToGroup.getNumGroups()];
        this.groupsSeen = new int[session.docIdToGroup.getNumGroups()];
        this.bitTree = new BitTree(session.docIdToGroup.getNumGroups());
        this.flamdexReader = flamdexReader;
        this.calculator = getCalculator();
    }

    @Override
    public abstract boolean nextField();

    @Override
    public void close() {
        calculator.close();
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
            if (groupPointer >= groupsSeenCount) {
                return false;
            }
            groupPointer++;
            return groupPointer < groupsSeenCount;
        }
        return calculator.calculateTermGroupStats();
    }

    private interface TermGroupStatsCalculator extends Closeable {
        boolean calculateTermGroupStats();

        @Override
        void close();
    }

    private class DefaultCalculator implements TermGroupStatsCalculator {
        private int[] docIdBuf;
        private int[] docGroupBuf;
        private long[] valBuf;
        private boolean closed = false;

        private DefaultCalculator() {
            docIdBuf = session.memoryPool.getIntBuffer(ImhotepLocalSession.BUFFER_SIZE, true);
            docGroupBuf = session.memoryPool.getIntBuffer(ImhotepLocalSession.BUFFER_SIZE, true);
            valBuf = session.memoryPool.getLongBuffer(ImhotepLocalSession.BUFFER_SIZE, true);
        }

        @Override
        public boolean calculateTermGroupStats() {
            // clear out ram from previous iterations if necessary
            for (final long[] x : termGrpStats) {
                ImhotepLocalSession.clear(x, groupsSeen, groupsSeenCount);
            }
            groupsSeenCount = 0;

            // this is the critical loop of all of imhotep, making this loop faster is very good....

            // todo: refactor AbstractFlamdexFTGSIterator to be independent with ImhotepLocalSession internals.
            // we do synchronization over session here,
            // because in fillDocIdBuffer() use session.docIdBuf
            // and docIdToGroup.nextGroupCallback use session.docGroupBuffer, session.docIdBuf,
            // session.numStats, session.statLookup, session.valBuf
            synchronized (session) {
                while (true) {
                    if (ImhotepLocalSession.logTiming) {
                        docsTime -= System.nanoTime();
                    }
                    final int n = fillDocIdBuffer(docIdBuf);
                    if (ImhotepLocalSession.logTiming) {
                        docsTime += System.nanoTime();
                        lookupsTime -= System.nanoTime();
                    }
                    session.docIdToGroup.nextGroupCallback(n, termGrpStats, bitTree, docIdBuf, valBuf, docGroupBuf);

                    if (ImhotepLocalSession.logTiming) {
                        lookupsTime += System.nanoTime();
                        timingErrorTime -= System.nanoTime();
                        timingErrorTime += System.nanoTime();
                    }
                    if (n < ImhotepLocalSession.BUFFER_SIZE) {
                        break;
                    }
                }
            }
            groupsSeenCount = bitTree.dump(groupsSeen);

            groupPointer = 0;
            resetGroupStats = false;
            return groupsSeenCount > 0;
        }

        @Override
        public void close() {
            if (closed) {
                return;
            }
            closed = true;
            session.memoryPool.returnIntBuffer(docIdBuf);
            session.memoryPool.returnIntBuffer(docGroupBuf);
            session.memoryPool.returnLongBuffer(valBuf);
            docIdBuf = null;
            docGroupBuf = null;
            valBuf = null;
        }
    }

    protected abstract int fillDocIdBuffer(final int[] docIdBuf);

    @Override
    public final int group() {
        return groupsSeen[groupPointer];
    }

    @Override
    public final void groupStats(final long[] stats) {
        final int group = group();
        for (int i = 0; i < session.numStats; i++) {
            stats[i] = termGrpStats[i][group];
        }
    }

    // All docs in one group (no zero group docs)
    // no stats or only one stat which is Count()
    private class ConstGroupCalculator implements TermGroupStatsCalculator {
        private final int group;
        private final boolean saveStat;
        private ConstGroupCalculator(final int group) {
            this.group = group;
            if ((session.numStats > 1)
                    || ((session.numStats == 1) && !(session.statLookup.get(0) instanceof Count)) ) {
                throw new IllegalStateException("Only no stats or Count() stat is expected");
            }
            this.saveStat = session.numStats == 1;
            // only one group can appear in result.
            // filling it here and managing existence of group with groupsSeenCount = 0 or 1
            groupsSeen[0] = group;
        }

        @Override
        public boolean calculateTermGroupStats() {
            if (group == 0) {
                // All docs are filtered out.
                // todo: check if all docs are filtered before
                // FTGSIterator creation and return empty iterator
                return false;
            }

            groupPointer = 0;
            resetGroupStats = false;

            if (ImhotepLocalSession.logTiming) {
                termFreqTime -= System.nanoTime();
            }
            final long n = termDocFreq();
            if (ImhotepLocalSession.logTiming) {
                termFreqTime += System.nanoTime();
            }
            if (saveStat) {
                termGrpStats[0][group] = n;
            }
            if (n > 0) {
                groupsSeenCount = 1;
                return true;
            } else {
                groupsSeenCount = 0;
                return false;
            }
        }

        @Override
        public void close() {
        }
    }

    // All documents in one group or in zero group, no stats
    private class BitSetGroupNoStatsCalculator implements TermGroupStatsCalculator {
        private int[] docIdBuf;
        private int[] docGroupBuf;
        private long[] valBuf;
        private boolean closed = false;

        private BitSetGroupNoStatsCalculator(final int group) {
            docIdBuf = session.memoryPool.getIntBuffer(ImhotepLocalSession.BUFFER_SIZE, true);
            docGroupBuf = session.memoryPool.getIntBuffer(ImhotepLocalSession.BUFFER_SIZE, true);
            valBuf = session.memoryPool.getLongBuffer(ImhotepLocalSession.BUFFER_SIZE, true);

            // only one group can appear in result.
            // filling it here and managing existence of group with groupsSeenCount = 0 or 1
            groupsSeen[0] = group;
        }

        @Override
        public boolean calculateTermGroupStats() {
            groupPointer = 0;
            resetGroupStats = false;

            synchronized (session) {
                while (true) {
                    if (ImhotepLocalSession.logTiming) {
                        docsTime -= System.nanoTime();
                    }
                    final int n = fillDocIdBuffer(docIdBuf);
                    if (ImhotepLocalSession.logTiming) {
                        docsTime += System.nanoTime();
                        lookupsTime -= System.nanoTime();
                    }
                    final int processed = session.docIdToGroup.nextGroupCallback(n, termGrpStats, bitTree, docIdBuf, valBuf, docGroupBuf);

                    if (ImhotepLocalSession.logTiming) {
                        lookupsTime += System.nanoTime();
                        timingErrorTime -= System.nanoTime();
                        timingErrorTime += System.nanoTime();
                    }
                    if (processed > 0) {
                        // at least one doc for this term exists.
                        // no need to iterate further, exiting
                        groupsSeenCount = 1;
                        return true;
                    }
                    if (n < ImhotepLocalSession.BUFFER_SIZE) {
                        // no docs for term
                        groupsSeenCount = 0;
                        return false;
                    }
                }
            }
        }

        @Override
        public void close() {
            if (closed) {
                return;
            }
            closed = true;
            session.memoryPool.returnIntBuffer(docIdBuf);
            session.memoryPool.returnIntBuffer(docGroupBuf);
            session.memoryPool.returnLongBuffer(valBuf);
            docIdBuf = null;
            docGroupBuf = null;
            valBuf = null;
        }
    }

    // choose calculator based on groups and stats
    private TermGroupStatsCalculator getCalculator() {
        if (session.docIdToGroup instanceof ConstantGroupLookup) {
            if ((session.numStats == 0)
                    || ((session.numStats == 1) && (session.statLookup.get(0) instanceof Count))) {
                if (isCountMethodsReliable(flamdexReader.get())) {
                    final int group = ((ConstantGroupLookup) session.docIdToGroup).getConstantGroup();
                    return new ConstGroupCalculator(group);
                }
            }
        }

        if (session.docIdToGroup instanceof BitSetGroupLookup) {
            if(session.numStats == 0) {
                return new BitSetGroupNoStatsCalculator(1);
            }
        }

        return new DefaultCalculator();
    }

    // Check if we can trust terms frequency and documents count information from flamdex.
    private static boolean isCountMethodsReliable(final FlamdexReader reader) {
        // These classes we trust.
        if ((reader instanceof LuceneFlamdexReader)
                || (reader instanceof SimpleFlamdexReader)
                || (reader instanceof MemoryFlamdex)
                || (reader instanceof MockFlamdexReader)
                || (reader instanceof RamsesFlamdexWrapper)) {
            return true;
        }

        // These classes might be wrong.
        if ((reader instanceof DynamicFlamdexReader)
                || (reader instanceof SegmentReader)) {
            return false;
        }

        // Unwrapping helper classes.
        if (reader instanceof CachedFlamdexReader) {
            return isCountMethodsReliable (((CachedFlamdexReader)reader).getWrapped());
        }

        if (reader instanceof CachedFlamdexReaderReference) {
            return isCountMethodsReliable(((CachedFlamdexReaderReference)reader).getReader().getWrapped());
        }

        if (reader instanceof InstrumentedFlamdexReader) {
            return isCountMethodsReliable(((InstrumentedFlamdexReader)reader).getWrapped());
        }

        // Don't trust unknown class
        return false;
    }
}

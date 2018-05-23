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

import com.google.common.base.Throwables;
import com.indeed.imhotep.AbstractImhotepMultiSession;
import com.indeed.imhotep.FTGSIteratorUtil;
import com.indeed.imhotep.FTGSSplitter;
import com.indeed.imhotep.GroupStatsDummyIterator;
import com.indeed.imhotep.ImhotepRemoteSession;
import com.indeed.imhotep.MemoryReservationContext;
import com.indeed.imhotep.RawFTGSMerger;
import com.indeed.imhotep.TermLimitedRawFTGSIterator;
import com.indeed.imhotep.api.FTGSIterator;
import com.indeed.imhotep.api.GroupStatsIterator;
import com.indeed.imhotep.api.RawFTGSIterator;
import com.indeed.imhotep.pool.GroupStatsPool;
import com.indeed.util.core.io.Closeables2;
import org.apache.log4j.Logger;

import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;


/**
 * @author jsgroth
 */
public class MTImhotepLocalMultiSession extends AbstractImhotepMultiSession<ImhotepLocalSession> {
    private static final Logger log = Logger.getLogger(MTImhotepLocalMultiSession.class);

    private final MemoryReservationContext memory;

    private final AtomicReference<Boolean> closed = new AtomicReference<>();

    protected FTGSSplitter[] ftgsIteratorSplitters;

    public MTImhotepLocalMultiSession(final ImhotepLocalSession[] sessions,
                                      final MemoryReservationContext memory,
                                      final AtomicLong tempFileSizeBytesLeft,
                                      final String userName,
                                      final String clientName)
    {
        super(sessions, tempFileSizeBytesLeft, userName, clientName);
        this.memory = memory;
        this.closed.set(false);
    }

    @Override
    protected void preClose() {
        if (closed.compareAndSet(false, true)) {
            super.preClose();
        }
    }

    @Override
    public long[] getGroupStats(final int stat) {

        final GroupStatsPool pool = new GroupStatsPool(numGroups);

        executeRuntimeException(nullBuf, session -> {
            final long[] buffer = pool.getBuffer();
            session.addGroupStats(stat, buffer);
            pool.returnBuffer(buffer);
            return null;
        });

        // TODO: run as a separate task maybe?
        return pool.getTotalResult();
    }

    @Override
    public GroupStatsIterator getGroupStatsIterator(final int stat) {
        // there is two ways to create GroupStatsIterator in multisession:
        // create iterator over result of getGroupStats method or create merger for iterators.
        // In case of local multisession we are keeping full result in memory anyway,
        // so first option is better.
        return new GroupStatsDummyIterator(getGroupStats(stat));
    }

    @Override
    public FTGSIterator getFTGSIterator(final String[] intFields, final String[] stringFields, final long termLimit, final int sortStat) {
        final boolean topTermsByStat = sortStat >= 0;
        final long perSplitTermLimit = topTermsByStat ? 0 : termLimit;
        return mergeFTGSIteratorsForSessions(sessions, termLimit, sortStat, s -> s.getFTGSIterator(intFields, stringFields, perSplitTermLimit));
    }

    @Override
    public RawFTGSIterator[] getFTGSIteratorSplits(String[] intFields, String[] stringFields, long termLimit) {
        throw new UnsupportedOperationException();
    }

    @Override
    public FTGSIterator getSubsetFTGSIterator(final Map<String, long[]> intFields, final Map<String, String[]> stringFields) {
        return mergeFTGSIteratorsForSessions(sessions, -1, -1, s -> s.getSubsetFTGSIterator(intFields, stringFields));
    }

    @Override
    public RawFTGSIterator[] getSubsetFTGSIteratorSplits(
            final Map<String, long[]> intFields,
            final Map<String, String[]> stringFields) {
        throw new UnsupportedOperationException();
    }

    @Override
    public synchronized RawFTGSIterator getFTGSIteratorSplit(final String[] intFields, final String[] stringFields, final int splitIndex, final int numSplits, final long termLimit) {
        if (ftgsIteratorSplitters == null || (ftgsIteratorSplitters.length == 0 && sessions.length != 0) ||
                ftgsIteratorSplitters[0].isClosed()) {

            ftgsIteratorSplitters = new FTGSSplitter[sessions.length];
            try {
                executeSessions(getSplitBufferThreads, ftgsIteratorSplitters, false, imhotepSession -> imhotepSession.getFTGSIteratorSplitter(intFields, stringFields, numSplits, termLimit));
            } catch (final Throwable t) {
                Closeables2.closeAll(log, ftgsIteratorSplitters);
                throw Throwables.propagate(t);
            }
        }

        final List<RawFTGSIterator> splits = Arrays.stream(ftgsIteratorSplitters)
                .map(splitter -> splitter.getFtgsIterators()[splitIndex]).collect(Collectors.toList());

        if (sessions.length == 1) {
            return splits.get(0);
        }
        RawFTGSIterator merger = new RawFTGSMerger(splits, numStats, null);
        if(termLimit > 0) {
            merger = new TermLimitedRawFTGSIterator(merger, termLimit);
        }
        return merger;
    }

    @Override
    public synchronized RawFTGSIterator getSubsetFTGSIteratorSplit(final Map<String, long[]> intFields, final Map<String, String[]> stringFields, final int splitIndex, final int numSplits) {
        if (ftgsIteratorSplitters == null || (ftgsIteratorSplitters.length == 0 && sessions.length != 0) ||
                ftgsIteratorSplitters[0].isClosed()) {

            ftgsIteratorSplitters = new FTGSSplitter[sessions.length];
            try {
                executeSessions(getSplitBufferThreads, ftgsIteratorSplitters, false, imhotepSession -> imhotepSession.getSubsetFTGSIteratorSplitter(intFields, stringFields, numSplits));
            } catch (final Throwable t) {
                Closeables2.closeAll(log, ftgsIteratorSplitters);
                throw Throwables.propagate(t);
            }
        }
        final List<RawFTGSIterator> splits = Arrays.stream(ftgsIteratorSplitters)
                .map(splitter -> splitter.getFtgsIterators()[splitIndex]).collect(Collectors.toList());

        if (sessions.length == 1) {
            return splits.get(0);
        }
        return new RawFTGSMerger(splits, numStats, null);
    }

    @Override
    public GroupStatsIterator getDistinct(final String field, final boolean isIntField) {
        if (sessions.length == 1) {
            return sessions[0].getDistinct(field, isIntField);
        }
        // It's a hack.
        // We don't care about stats while calculating distinct.
        // And FTGS with no-stats is faster.
        // So we drop stats count, calculate distinct and return stats.
        final int[] savedNumStats = new int[sessions.length];
        for (int i = 0; i < sessions.length; i++) {
            savedNumStats[i] = sessions[i].numStats;
        }
        final GroupStatsIterator result;
        try {
            // drop stats.
            for (final ImhotepLocalSession session : sessions) {
                session.numStats = 0;
            }
            final String[] intFields = isIntField ? new String[]{field} : new String[0];
            final String[] strFields = isIntField ? new String[0] : new String[]{field};
            final FTGSIterator iterator = getFTGSIterator(intFields, strFields);
            result = FTGSIteratorUtil.calculateDistinct(iterator, getNumGroups());
        } finally {
            // return stats back.
            for (int i = 0; i < sessions.length; i++) {
                sessions[i].numStats = savedNumStats[i];
            }
        }
        return result;
    }

    @Override
    protected void postClose() {
        if (memory.usedMemory() > 0) {
            log.error("MTImhotepMultiSession is leaking! usedMemory = "+memory.usedMemory());
        }
        Closeables2.closeQuietly(memory, log);
        super.postClose();
    }

    @Override
    protected ImhotepRemoteSession createImhotepRemoteSession(final InetSocketAddress address,
                                                              final String sessionId,
                                                              final AtomicLong tempFileSizeBytesLeft) {
        return new ImhotepRemoteSession(address.getHostName(), address.getPort(),
                                        sessionId, tempFileSizeBytesLeft);
    }
}

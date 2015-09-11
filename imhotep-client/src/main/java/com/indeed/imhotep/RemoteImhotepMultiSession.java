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
 package com.indeed.imhotep;

import com.google.common.base.Throwables;
import com.indeed.imhotep.api.FTGSIterator;
import com.indeed.imhotep.api.ImhotepSession;
import com.indeed.imhotep.api.RawFTGSIterator;
import com.indeed.util.core.Throwables2;
import com.indeed.util.core.Pair;
import org.apache.log4j.Logger;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author jsgroth
 */
public class RemoteImhotepMultiSession extends AbstractImhotepMultiSession<ImhotepSession> {
    private static final Logger log = Logger.getLogger(RemoteImhotepMultiSession.class);

    private final String sessionId;
    private final InetSocketAddress[] nodes;

    private final long localTempFileSizeLimit;

    public RemoteImhotepMultiSession(ImhotepSession[] sessions,
                                     final String sessionId,
                                     final InetSocketAddress[] nodes,
                                     long localTempFileSizeLimit,
                                     AtomicLong tempFileSizeBytesLeft) {
        super(sessions, tempFileSizeBytesLeft);

        this.sessionId = sessionId;
        this.nodes = nodes;
        this.localTempFileSizeLimit = localTempFileSizeLimit;
    }

    @Override
    public FTGSIterator getFTGSIterator(final String[] intFields, final String[] stringFields) {
        if (sessions.length == 1) {
            return sessions[0].getFTGSIterator(intFields, stringFields);
        }
        final RawFTGSIterator[] mergers = getFTGSIteratorSplits(intFields, stringFields);
        return new FTGSInterleaver(mergers);
    }

    public RawFTGSIterator[] getFTGSIteratorSplits(final String[] intFields, final String[] stringFields) {
        final Pair<Integer, ImhotepSession>[] indexesAndSessions = new Pair[sessions.length];
        for (int i = 0; i < sessions.length; i++) {
            indexesAndSessions[i] = Pair.of(i, sessions[i]);
        }
        final RawFTGSIterator[] mergers = new RawFTGSIterator[sessions.length];
        try {
            execute(mergers, indexesAndSessions, new ThrowingFunction<Pair<Integer, ImhotepSession>, RawFTGSIterator>() {
                public RawFTGSIterator apply(final Pair<Integer, ImhotepSession> indexSessionPair) throws Exception {
                    final ImhotepSession session = indexSessionPair.getSecond();
                    final int index = indexSessionPair.getFirst();
                    return session.mergeFTGSSplit(intFields, stringFields, sessionId, nodes, index);
                }
            });
        } catch (ExecutionException e) {
            throw Throwables.propagate(e);
        }
        return mergers;
    }

    @Override
    public FTGSIterator getSubsetFTGSIterator(final Map<String, long[]> intFields, final Map<String, String[]> stringFields) {
        if (sessions.length == 1) {
            return sessions[0].getSubsetFTGSIterator(intFields, stringFields);
        }
        final RawFTGSIterator[] mergers = getSubsetFTGSIteratorSplits(intFields, stringFields);
        return new FTGSInterleaver(mergers);
    }

    @Override
    public RawFTGSIterator[] getSubsetFTGSIteratorSplits(final Map<String, long[]> intFields, final Map<String, String[]> stringFields) {
        final Pair<Integer, ImhotepSession>[] indexesAndSessions = new Pair[sessions.length];
        for (int i = 0; i < sessions.length; i++) {
            indexesAndSessions[i] = Pair.of(i, sessions[i]);
        }
        final RawFTGSIterator[] mergers = new RawFTGSIterator[sessions.length];
        try {
            execute(mergers, indexesAndSessions, new ThrowingFunction<Pair<Integer, ImhotepSession>, RawFTGSIterator>() {
                public RawFTGSIterator apply(final Pair<Integer, ImhotepSession> indexSessionPair) throws Exception {
                    final ImhotepSession session = indexSessionPair.getSecond();
                    final int index = indexSessionPair.getFirst();
                    return session.mergeSubsetFTGSSplit(intFields, stringFields, sessionId, nodes, index);
                }
            });
        } catch (ExecutionException e) {
            throw Throwables.propagate(e);
        }
        return mergers;
    }

    /**
     * Returns the number of bytes written to the temp files for this session locally.
     * Returns -1 if tempFileSizeBytesLeft was set to null.
     */
    public long getTempFilesBytesWritten() {
        if(tempFileSizeBytesLeft == null || localTempFileSizeLimit <= 0) {
            return -1;
        }
        return localTempFileSizeLimit - tempFileSizeBytesLeft.get();
    }

    @Override
    public void writeFTGSIteratorSplit(String[] intFields, String[] stringFields, int splitIndex, int numSplits, final Socket socket) {
        throw new UnsupportedOperationException("");
    }

    @Override
    protected ImhotepRemoteSession createImhotepRemoteSession(InetSocketAddress address, String sessionId, AtomicLong tempFileSizeBytesLeft) {
        throw new UnsupportedOperationException("RemoteImhotepMultiSession doesn't open any remote imhotep connections!");
    }
}

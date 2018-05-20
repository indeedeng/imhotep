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

import com.google.common.io.ByteStreams;
import com.indeed.imhotep.AbstractImhotepMultiSession;
import com.indeed.imhotep.FTGSIteratorUtil;
import com.indeed.imhotep.ImhotepRemoteSession;
import com.indeed.imhotep.MemoryReservationContext;
import com.indeed.imhotep.api.FTGSIterator;
import com.indeed.imhotep.api.GroupStatsIterator;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.imhotep.api.RawFTGSIterator;
import com.indeed.util.core.io.Closeables2;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;


/**
 * @author jsgroth
 */
public class MTImhotepLocalMultiSession extends AbstractImhotepMultiSession<ImhotepLocalSession> {
    private static final Logger log = Logger.getLogger(MTImhotepLocalMultiSession.class);

    static {
        loadNativeLibrary();
        log.info("libftgs loaded");
        log.info("Using SSSE3! (if the processor in this computer doesn't support SSSE3 "
                         + "this process will fail with SIGILL)");
    }

    public static void loadNativeLibrary() {
        try {
            final String osName = System.getProperty("os.name");
            final String arch = System.getProperty("os.arch");
            final String resourcePath = "/native/" + osName + "-" + arch + "/libftgs.so.1.0.1";
            final InputStream is = MTImhotepLocalMultiSession.class.getResourceAsStream(resourcePath);
            if (is == null) {
                throw new FileNotFoundException(
                        "unable to find libftgs.so.1.0.1 at resource path " + resourcePath);
            }
            final File tempFile = File.createTempFile("libftgs", ".so");
            final OutputStream os = new FileOutputStream(tempFile);
            ByteStreams.copy(is, os);
            os.close();
            is.close();
            System.load(tempFile.getAbsolutePath());
            tempFile.delete();
        } catch (final Throwable e) {
            e.printStackTrace();
            log.warn("unable to load libftgs using class loader, looking in java.library.path", e);
            System.loadLibrary("ftgs"); // if this fails it throws UnsatisfiedLinkError
        }
    }

    private final MemoryReservationContext memory;

    private final AtomicReference<Boolean> closed = new AtomicReference<>();

    private final long memoryClaimed;

    public MTImhotepLocalMultiSession(final ImhotepLocalSession[] sessions,
                                      final MemoryReservationContext memory,
                                      final AtomicLong tempFileSizeBytesLeft)
        throws ImhotepOutOfMemoryException {
        super(sessions, tempFileSizeBytesLeft);
        this.memory = memory;
        this.memoryClaimed = 0;
        this.closed.set(false);

        if (!memory.claimMemory(memoryClaimed)) {
            //noinspection NewExceptionWithoutArguments
            throw new ImhotepOutOfMemoryException();
        }
    }

    @Override
    protected void preClose() {
        if (closed.compareAndSet(false, true)) {
            try {
                super.preClose();
            } finally {
                memory.releaseMemory(memoryClaimed);
                // don't want to shut down the executor since it is re-used
            }
        }
    }

    @Override
    public FTGSIterator getFTGSIterator(final String[] intFields, final String[] stringFields, final long termLimit, final int sortStat) {
        final boolean topTermsByStat = sortStat >= 0;
        final long perSplitTermLimit = topTermsByStat ? 0 : termLimit;
        return mergeFTGSIteratorsForSessions(sessions, termLimit, sortStat, s -> s.getFTGSIterator(intFields, stringFields, perSplitTermLimit));
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

    public RawFTGSIterator[] getFTGSIteratorSplits(final String[] intFields,
                                                   final String[] stringFields,
                                                   final long termLimit) {
        throw new UnsupportedOperationException();
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

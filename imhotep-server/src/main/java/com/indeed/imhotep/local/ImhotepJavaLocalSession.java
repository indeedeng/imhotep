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

import com.indeed.flamdex.api.FlamdexReader;
import com.indeed.flamdex.api.IntValueLookup;
import com.indeed.imhotep.ImhotepMemoryPool;
import com.indeed.imhotep.MemoryReservationContext;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.imhotep.metrics.Constant;
import com.indeed.imhotep.metrics.Count;
import org.apache.log4j.Logger;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public class ImhotepJavaLocalSession extends ImhotepLocalSession {

    private static final Logger log = Logger.getLogger(ImhotepJavaLocalSession.class);

    public ImhotepJavaLocalSession(final String sessionId, final FlamdexReader flamdexReader, @Nullable final Interval shardTimeRange)
        throws ImhotepOutOfMemoryException {
        this(sessionId, flamdexReader, shardTimeRange,
             new MemoryReservationContext(new ImhotepMemoryPool(Long.MAX_VALUE)), null);
    }

    public ImhotepJavaLocalSession(final String sessionId,
                                   final FlamdexReader flamdexReader,
                                   @Nullable final Interval shardTimeRange,
                                   final MemoryReservationContext memory,
                                   final AtomicLong tempFileSizeBytesLeft)
        throws ImhotepOutOfMemoryException {

        super(sessionId, flamdexReader, shardTimeRange, memory, tempFileSizeBytesLeft);
    }

    @Override
    protected void addGroupStats(final GroupLookup docIdToGroup, final List<String> stat, final long[] partialResult) throws ImhotepOutOfMemoryException {
        if (docIdToGroup.isFilteredOut()) {
            return;
        }

        final int[] docIdBuffer = memoryPool.getIntBuffer(ImhotepLocalSession.BUFFER_SIZE, true);
        final int[] docGroupBuffer = memoryPool.getIntBuffer(ImhotepLocalSession.BUFFER_SIZE, true);
        final long[] valueBuffer = memoryPool.getLongBuffer(ImhotepLocalSession.BUFFER_SIZE, true);

        try (final MetricStack stack = new MetricStack()) {
            final IntValueLookup lookup = stack.push(stat);
            updateGroupStatsAllDocs(
                    lookup,
                    partialResult,
                    docIdToGroup,
                    docGroupBuffer,
                    docIdBuffer,
                    valueBuffer
            );
        }
        memoryPool.returnIntBuffer(docIdBuffer);
        memoryPool.returnIntBuffer(docGroupBuffer);
        memoryPool.returnLongBuffer(valueBuffer);
    }

    private static void updateGroupStatsAllDocs(final IntValueLookup statLookup,
                                                final long[]         results,
                                                final GroupLookup    docIdToGroup,
                                                final int[]          docGrpBuffer,
                                                final int[]          docIdBuf,
                                                final long[]         valBuf) {
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

    static void updateGroupStatsDocIdBuf(final IntValueLookup statLookup,
                                         final long[] results,
                                         final int[] docGrpBuffer,
                                         final int[] docIdBuf,
                                         final long[] valBuf,
                                         final int n) {
        /* This is a hacky optimization, but probably worthwhile, since Count is
         * such a common stat. Rather than have Count fill up an array with ones
         * and then add each in turn to our results, we just increment the
         * results directly. */
        if (statLookup instanceof Count) {
            for (int i = 0; i < n; i++) {
                results[docGrpBuffer[i]] += 1;
            }
        } else if (statLookup instanceof Constant) {
            final long value = ((Constant) statLookup).getValue();
            for (int i = 0; i < n; i++) {
                results[docGrpBuffer[i]] += value;
            }
        } else {
            statLookup.lookup(docIdBuf, valBuf, n);
            for (int i = 0; i < n; i++) {
                results[docGrpBuffer[i]] += valBuf[i];
            }
        }
        results[0] = 0; // clearing value for filtered-out group.
    }
}

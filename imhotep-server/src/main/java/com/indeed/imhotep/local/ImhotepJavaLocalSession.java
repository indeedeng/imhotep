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

import com.indeed.flamdex.api.*;
import com.indeed.imhotep.*;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;

import org.apache.log4j.Logger;

import java.util.concurrent.atomic.AtomicLong;

public class ImhotepJavaLocalSession extends ImhotepLocalSession {

    static final Logger log = Logger.getLogger(ImhotepJavaLocalSession.class);

    public ImhotepJavaLocalSession(final FlamdexReader flamdexReader)
        throws ImhotepOutOfMemoryException {
        this(flamdexReader, null,
             new MemoryReservationContext(new ImhotepMemoryPool(Long.MAX_VALUE)),
             false, null);
    }

    public ImhotepJavaLocalSession(FlamdexReader flamdexReader, boolean optimizeGroupZeroLookups)
        throws ImhotepOutOfMemoryException {
        this(flamdexReader, null,
             new MemoryReservationContext(new ImhotepMemoryPool(Long.MAX_VALUE)),
             optimizeGroupZeroLookups, null);
    }

    public ImhotepJavaLocalSession(final FlamdexReader flamdexReader,
                                   String optimizedIndexDirectory,
                                   final MemoryReservationContext memory,
                                   boolean optimizeGroupZeroLookups,
                                   AtomicLong tempFileSizeBytesLeft)
        throws ImhotepOutOfMemoryException {

        super(flamdexReader, optimizedIndexDirectory, memory,
              optimizeGroupZeroLookups, tempFileSizeBytesLeft);
    }

    @Override
    public synchronized long[] getGroupStats(int stat) {
        if (groupStats.isDirty(stat)) {
            updateGroupStatsAllDocs(statLookup.get(stat),
                                    groupStats.get(stat),
                                    docIdToGroup,
                                    docGroupBuffer,
                                    docIdBuf,
                                    valBuf);
            groupStats.validate(stat);
        }
        return groupStats.get(stat);
    }

    private static void updateGroupStatsAllDocs(IntValueLookup statLookup,
                                                long[]         results,
                                                GroupLookup    docIdToGroup,
                                                int[]          docGrpBuffer,
                                                int[]          docIdBuf,
                                                long[]         valBuf) {
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

    static void updateGroupStatsDocIdBuf(IntValueLookup statLookup,
                                         long[] results,
                                         int[] docGrpBuffer,
                                         int[] docIdBuf,
                                         long[] valBuf,
                                         int n) {
        statLookup.lookup(docIdBuf, valBuf, n);
        for (int i = 0; i < n; i++) {
            results[docGrpBuffer[i]] += valBuf[i];
        }
    }
}

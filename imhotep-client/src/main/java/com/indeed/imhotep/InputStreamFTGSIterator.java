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
 package com.indeed.imhotep;

import com.google.common.annotations.VisibleForTesting;
import com.indeed.imhotep.FTGSBinaryFormat.FieldStat;
import com.indeed.imhotep.api.FTGSIterator;
import org.apache.log4j.Logger;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.InputStream;

public class InputStreamFTGSIterator extends AbstractInputStreamFTGSIterator implements FTGSIterator {

    private static final Logger log = Logger.getLogger(InputStreamFTGSIterator.class);

    private final int numStats;
    private final long[] statsBuf;

    public InputStreamFTGSIterator(final InputStream in,
                                   @Nullable final FieldStat[] fieldsStats,
                                   final int numStats,
                                   final int numGroups) {
        super(in, fieldsStats, numGroups);
        this.numStats = numStats;
        this.statsBuf = new long[numStats];
    }

    @VisibleForTesting
    InputStreamFTGSIterator(final InputStream in,
                            final int numStats,
                            final int numGroups) {
        this(in, null, numStats, numGroups);
    }

    @Override
    public int getNumStats() {
        return numStats;
    }

    @Override
    final void readStats() {
        try {
            for (int i = 0; i < statsBuf.length; i++) {
                statsBuf[i] = readSVLong();
            }
        } catch (final IOException e) {
            iteratorStatus = -1;
            throw new RuntimeException(e);
        }
    }

    @Override
    public final void groupStats(final long[] stats) {
        System.arraycopy(statsBuf, 0, stats, 0, statsBuf.length);
    }
}

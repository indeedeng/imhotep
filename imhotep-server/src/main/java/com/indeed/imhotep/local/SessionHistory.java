/*
 * Copyright (C) 2015 Indeed Inc.
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
import com.indeed.imhotep.client.ShardTimeUtils;
import com.indeed.imhotep.service.SessionHistoryIf;

import org.apache.log4j.Logger;

import org.joda.time.DateTime;
import org.joda.time.Interval;

import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

public class SessionHistory implements SessionHistoryIf {

    private static final Logger log = Logger.getLogger(SessionHistory.class);

    /** Useful in test scenarios in which we just want to stub out SessionHistoryIf. */
    public static class Null implements SessionHistoryIf {
        public String getSessionId() { return ""; }
        public void onCreate(FlamdexReader reader) { }
        public void onGetFTGSIterator(String[] intFields, String[] stringFields) { }
        public void onWriteFTGSIteratorSplit(String[] intFields, String[] stringFields) { }
        public void onPushStat(String stat, IntValueLookup lookup) { }
    }

    private static class ShardInfo {
        final DateTime date;
        final long     sizeInBytes;

        public ShardInfo(final DateTime date, final long sizeInBytes) {
            this.date        = date;
            this.sizeInBytes = sizeInBytes;
        }
    }

    private final Map<String, ShardInfo> shardInfoMap = new TreeMap<String, ShardInfo>();
    private final Map<String, Integer>    statsPushed = new TreeMap<String, Integer>();

    private final Set<String> datasetsUsed = new TreeSet<String>();
    private final Set<String> intFields    = new TreeSet<String>();
    private final Set<String> stringFields = new TreeSet<String>();

    private final String sessionId;

    public SessionHistory(final String sessionId) {
        this.sessionId = sessionId;
    }

    @Override
    public String getSessionId() { return sessionId; }

    @Override
    public void onCreate(FlamdexReader reader) {
        /* Capture the date of this shard. */
        final String shardId = reader.getDirectory();
        if (!shardInfoMap.containsKey(shardId)) {
            try {
                final Interval interval = ShardTimeUtils.parseInterval(shardId);
                final DateTime start    = interval.getStart();
                final DateTime date     = new DateTime(start.year().get(),
                                                       start.monthOfYear().get(),
                                                       start.dayOfMonth().get(),
                                                       0, 0);
                final long sizeInBytes = 0; // !@# compute shard size
                shardInfoMap.put(shardId, new ShardInfo(date, sizeInBytes));
            }
            catch (Exception ex) {
                // !@# not sure this even merits a warning...
                log.warn("cannot extract date from shard directory", ex);
            }
        }
    }

    @Override
    public void onGetFTGSIterator(String[] intFields, String[] stringFields) {
        this.intFields.addAll(Arrays.asList(intFields));
        this.stringFields.addAll(Arrays.asList(stringFields));
    }

    @Override
    public void onWriteFTGSIteratorSplit(String[] intFields, String[] stringFields) {
        this.intFields.addAll(Arrays.asList(intFields));
        this.stringFields.addAll(Arrays.asList(stringFields));
    }

    @Override
    public void onPushStat(String stat, IntValueLookup lookup) {
        final long    range        = lookup.getMax() - lookup.getMin();
        final int     leadingZeros = 64 - Long.numberOfLeadingZeros(range);
        final Integer sizeInBytes  = (leadingZeros + 7) / 8;

        final Integer current = statsPushed.get(stat);
        statsPushed.put(stat, current != null ?
                        Math.max(sizeInBytes, current) : sizeInBytes);
    }
}

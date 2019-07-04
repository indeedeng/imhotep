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

package com.indeed.imhotep.scheduling;

import java.util.Iterator;
import java.util.LinkedList;

/**
 *
 */
class ConsumptionTracker {
    private final long historyLengthNanos;
    private final long batchNanos;
    private final LinkedList<TimestampedConsumption> history = new LinkedList<>();
    /** Total consumption on this tracker for the history duration */
    private long sum = 0;

    ConsumptionTracker(long historyLengthNanos, long batchNanos) {
        this.historyLengthNanos = historyLengthNanos;
        this.batchNanos = batchNanos;
    }

    /**
     * We consider the consumption tracker active if it has consumption entries within the current history length
     *
     * @return boolean true if the consumption tracker has activity within the current history window, otherwise false
     */
    synchronized boolean isActive(final long nanoTime) {
        if (history.isEmpty()) {
            return false;
        }
        long lastTimeToKeep = nanoTime - historyLengthNanos;
        long latestTimeRecorded = history.getLast().timestamp;
        return latestTimeRecorded > lastTimeToKeep;
    }

    synchronized void record(long consumption) {
        sum += consumption;
        final TimestampedConsumption lastRecord = history.isEmpty() ? null : history.getLast();
        final long currentTime = System.nanoTime();
        if(lastRecord != null && lastRecord.timestamp / batchNanos == currentTime / batchNanos) {
            lastRecord.consumption += consumption;
        } else {
            history.addLast(new TimestampedConsumption(currentTime, consumption));
        }

        trim(currentTime);
    }

    synchronized long getConsumption(final long nanoTime) {
        trim(nanoTime);
        return sum;
    }

    private synchronized void trim(final long nanoTime) {
        long lastTimeToKeep = nanoTime - historyLengthNanos;
        for(Iterator<TimestampedConsumption> historyIterator = history.iterator(); historyIterator.hasNext(); ) {
            final TimestampedConsumption timestampedConsumption = historyIterator.next();
            if(timestampedConsumption.timestamp < lastTimeToKeep) {
                sum -= timestampedConsumption.consumption;
                historyIterator.remove();
            } else {
                break;
            }
        }
    }

    private static class TimestampedConsumption {
        final long timestamp;
        long consumption;

        private TimestampedConsumption(long timestamp, long consumption) {
            this.timestamp = timestamp;
            this.consumption = consumption;
        }

    }
}

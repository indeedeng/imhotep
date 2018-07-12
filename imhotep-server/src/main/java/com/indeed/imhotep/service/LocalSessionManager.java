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
 package com.indeed.imhotep.service;

import com.indeed.flamdex.api.FlamdexOutOfMemoryException;
import com.indeed.flamdex.api.FlamdexReader;
import com.indeed.imhotep.ImhotepStatusDump;
import com.indeed.imhotep.MemoryReservationContext;
import com.indeed.imhotep.local.MTImhotepLocalMultiSession;
import com.indeed.util.core.threads.NamedThreadFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * @author jsgroth
 *
 * this class is thread-safe
 */
public final class LocalSessionManager extends AbstractSessionManager<Map<ShardId, FlamdexReader>> {

    private final MetricStatsEmitter statsEmitter;

    private static final int REPORTING_FREQUENCY_MILLIS = 100;

    public LocalSessionManager(final MetricStatsEmitter statsEmitter, final int maxSessionsTotal, final int maxSessionsPerUser) {
        super(maxSessionsTotal, maxSessionsPerUser);
        this.statsEmitter = statsEmitter;
        final ScheduledExecutorService statsReportingExecutor =
                Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory("sessionManagerStatsReporter"));
        statsReportingExecutor.scheduleAtFixedRate(this::reportStats, REPORTING_FREQUENCY_MILLIS, REPORTING_FREQUENCY_MILLIS, TimeUnit.MILLISECONDS);
    }

    public void reportStats() {
        statsEmitter.histogram("active.sessions", getSessionCount());
        statsEmitter.histogram("active.users", getActiveUserCount());
    }

    @Override
    public void addSession(
            final String sessionId,
            final MTImhotepLocalMultiSession imhotepSession,
            final Map<ShardId, FlamdexReader> flamdexes,
            final String username,
            final String clientName,
            final String ipAddress,
            final int clientVersion,
            final String dataset,
            final long sessionTimeout,
            final MemoryReservationContext sessionMemoryContext) {
        final Session<Map<ShardId, FlamdexReader>> session = new Session<>(imhotepSession, flamdexes, username, clientName, ipAddress, clientVersion, dataset, sessionTimeout, sessionMemoryContext);
        addSession(sessionId, session);
    }

    public List<String> getShardIdsForSession(final String sessionId) {
        final Session<Map<ShardId, FlamdexReader>> session = internalGetSession(sessionId);
        final List<String> ret = new ArrayList<>(session.sessionState.size());
        for (final ShardId flamdex : session.sessionState.keySet()) {
            ret.add(flamdex.getId());
        }
        return ret;
    }

    public List<ImhotepStatusDump.SessionDump> getSessionDump() {
        final Map<String, Session<Map<ShardId, FlamdexReader>>> clone = cloneSessionMap();

        final List<ImhotepStatusDump.SessionDump> openSessions = new ArrayList<>(clone.size());
        for (final String sessionId : clone.keySet()) {
            final Session<Map<ShardId, FlamdexReader>> session = clone.get(sessionId);
            final List<ImhotepStatusDump.ShardDump> openShards = new ArrayList<>();
            for (final Map.Entry<ShardId, FlamdexReader> entry : session.sessionState.entrySet()) {
                try {
                    openShards.add(new ImhotepStatusDump.ShardDump(entry.getKey().getId(), entry.getKey().getDataset(), entry.getValue().getNumDocs(), computeMetricDump(entry.getValue())));
                } catch (FlamdexOutOfMemoryException e) {
                    log.error(e.getMessage(), e);
                }
            }
            openSessions.add(new ImhotepStatusDump.SessionDump(sessionId, session.dataset, "", session.username, session.clientName,
                    session.ipAddress, session.clientVersion, session.creationTime, openShards,
                    session.sessionMemoryContext.usedMemory(), session.sessionMemoryContext.getGlobalMaxUsedMemory()));
        }
        return openSessions;
    }

    private List<ImhotepStatusDump.MetricDump> computeMetricDump(FlamdexReader value) throws FlamdexOutOfMemoryException {
        final List<ImhotepStatusDump.MetricDump> toReturn = new ArrayList<>();
        for(String metric: value.getAvailableMetrics()) {
            toReturn.add(new ImhotepStatusDump.MetricDump(metric, value.getMetric(metric).memoryUsed()));
        }
        return toReturn;
    }
}

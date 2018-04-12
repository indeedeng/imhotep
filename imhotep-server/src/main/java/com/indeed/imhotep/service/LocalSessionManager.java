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

import com.indeed.imhotep.ImhotepStatusDump;
import com.indeed.imhotep.MemoryReservationContext;
import com.indeed.imhotep.api.ImhotepSession;
import com.indeed.util.varexport.VarExporter;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author jsgroth
 *
 * this class is thread-safe
 */
public final class LocalSessionManager extends AbstractSessionManager<Map<ShardId, CachedFlamdexReaderReference>> {

    public LocalSessionManager() {
        VarExporter.forNamespace(getClass().getSimpleName()).includeInGlobal().export(this, "");
    }

    @Override
    public void addSession(
            final String sessionId,
            final ImhotepSession imhotepSession,
            final Map<ShardId, CachedFlamdexReaderReference> flamdexes,
            final String username,
            final String clientName,
            final String ipAddress,
            final int clientVersion,
            final String dataset,
            final long sessionTimeout,
            final MemoryReservationContext sessionMemoryContext) {
        final Session<Map<ShardId, CachedFlamdexReaderReference>> session = new Session<>(imhotepSession, flamdexes, username, clientName, ipAddress, clientVersion, dataset, sessionTimeout, sessionMemoryContext);
        addSession(sessionId, session);
    }

    public List<String> getShardIdsForSession(final String sessionId) {
        final Session<Map<ShardId, CachedFlamdexReaderReference>> session = internalGetSession(sessionId);
        final List<String> ret = new ArrayList<>(session.sessionState.size());
        for (final ShardId flamdex : session.sessionState.keySet()) {
            ret.add(flamdex.getId());
        }
        return ret;
    }

    public List<ImhotepStatusDump.SessionDump> getSessionDump() {
        final Map<String, Session<Map<ShardId, CachedFlamdexReaderReference>>> clone = cloneSessionMap();

        final List<ImhotepStatusDump.SessionDump> openSessions = new ArrayList<>(clone.size());
        for (final String sessionId : clone.keySet()) {
            final Session<Map<ShardId, CachedFlamdexReaderReference>> session = clone.get(sessionId);
            final List<ImhotepStatusDump.ShardDump> openShards = new ArrayList<>();
            for (final Map.Entry<ShardId, CachedFlamdexReaderReference> entry : session.sessionState.entrySet()) {
                openShards.add(new ImhotepStatusDump.ShardDump(entry.getKey().getId(), entry.getKey().getDataset(), entry.getValue().getNumDocs(), entry.getValue().getMetricDump()));
            }
            openSessions.add(new ImhotepStatusDump.SessionDump(sessionId, session.dataset, "", session.username, session.clientName,
                    session.ipAddress, session.clientVersion, session.creationTime, openShards,
                    session.sessionMemoryContext.usedMemory(), session.sessionMemoryContext.getGlobalMaxUsedMemory()));
        }
        return openSessions;
    }
}

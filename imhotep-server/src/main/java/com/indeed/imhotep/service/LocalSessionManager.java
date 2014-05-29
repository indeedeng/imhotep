package com.indeed.imhotep.service;

import com.indeed.util.varexport.VarExporter;
import com.indeed.imhotep.ImhotepStatusDump;
import com.indeed.imhotep.api.ImhotepSession;

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
            final String ipAddress,
            final int clientVersion,
            final String dataset
    ) {
        final Session<Map<ShardId, CachedFlamdexReaderReference>> session = new Session(imhotepSession, flamdexes, username, ipAddress, clientVersion, dataset);
        addSession(sessionId, session);
    }

    public List<String> getShardIdsForSession(final String sessionId) {
        final Session<Map<ShardId, CachedFlamdexReaderReference>> session = internalGetSession(sessionId);
        final List<String> ret = new ArrayList<String>(session.sessionState.size());
        for (final ShardId flamdex : session.sessionState.keySet()) {
            ret.add(flamdex.getId());
        }
        return ret;
    }

    public List<ImhotepStatusDump.SessionDump> getSessionDump() {
        final Map<String, Session<Map<ShardId, CachedFlamdexReaderReference>>> clone = cloneSessionMap();

        final List<ImhotepStatusDump.SessionDump> openSessions = new ArrayList<ImhotepStatusDump.SessionDump>(clone.size());
        for (final String sessionId : clone.keySet()) {
            final Session<Map<ShardId, CachedFlamdexReaderReference>> session = clone.get(sessionId);
            final List<ImhotepStatusDump.ShardDump> openShards = new ArrayList<ImhotepStatusDump.ShardDump>();
            for (Map.Entry<ShardId, CachedFlamdexReaderReference> entry : session.sessionState.entrySet()) {
                openShards.add(new ImhotepStatusDump.ShardDump(entry.getKey().getId(), entry.getKey().getDataset(), entry.getValue().getNumDocs(), entry.getValue().getMetricDump()));
            }
            openSessions.add(new ImhotepStatusDump.SessionDump(sessionId, session.dataset, "", session.username, session.ipAddress, session.clientVersion, openShards));
        }
        return openSessions;
    }
}

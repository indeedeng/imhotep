package com.indeed.imhotep.service;

import com.indeed.imhotep.api.ImhotepSession;
import org.apache.log4j.Logger;

import java.util.List;

/**
 * @author jplaisance
 */
public final class RemoteSessionManager extends AbstractSessionManager<List<String>> {

    private static final Logger log = Logger.getLogger(RemoteSessionManager.class);

    @Override
    public void addSession(
            final String sessionId,
            final ImhotepSession imhotepSession,
            final List<String> sessionShardIds,
            final String username,
            final String ipAddress,
            final int clientVersion,
            final String dataset
    ) {
        final Session<List<String>> session = new Session<List<String>>(imhotepSession, sessionShardIds, username, ipAddress, clientVersion, dataset);
        addSession(sessionId, session);
    }

    public List<String> getShardIdsForSession(String sessionId) {
        return internalGetSession(sessionId).sessionState;
    }
}

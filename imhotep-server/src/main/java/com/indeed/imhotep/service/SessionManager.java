package com.indeed.imhotep.service;

import com.indeed.util.core.reference.SharedReference;
import com.indeed.imhotep.api.ImhotepSession;

import java.util.Map;

/**
 * @author jplaisance
 */
public interface SessionManager<E> {

    void addSession(
            String sessionId,
            ImhotepSession imhotepSession,
            E sessionState,
            String username,
            String ipAddress,
            int clientVersion,
            String dataset
    );

    SharedReference<ImhotepSession> getSession(String sessionId);

    boolean sessionIsValid(String sessionId);

    void removeAndCloseIfExists(String sessionId);

    void setNumStats(String sessionId, int newNumStats);

    int getNumStats(String sessionId);

    Map<String, Long> getLastActionTimes();
}

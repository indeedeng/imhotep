package com.indeed.imhotep.service;

import com.google.common.cache.CacheBuilder;
import com.indeed.util.core.io.Closeables2;
import com.indeed.util.core.reference.SharedReference;
import com.indeed.util.varexport.Export;
import com.indeed.imhotep.api.ImhotepSession;

import org.apache.log4j.Logger;

import java.util.HashMap;
import java.util.Map;

/**
 * @author jplaisance
 */
public abstract class AbstractSessionManager<E> implements SessionManager<E> {

    private static final Logger log = Logger.getLogger(AbstractSessionManager.class);

    private final Map<String, Session<E>> sessionMap = new HashMap<String, Session<E>>();
    private final Map<String, Exception> failureCauseMap = CacheBuilder.newBuilder().maximumSize(200).<String, Exception>build().asMap();

    protected void addSession(String sessionId, Session<E> session) {
        synchronized (sessionMap) {
            if (sessionMap.containsKey(sessionId)) {
                throw new IllegalArgumentException("there already exists a session with id "+sessionId);
            }
            sessionMap.put(sessionId, session);
        }
    }

    @Override
    public SharedReference<ImhotepSession> getSession(final String sessionId) {
        final Session<E> session = internalGetSession(sessionId);
        return session.imhotepSession.copy();
    }

    @Override
    public boolean sessionIsValid(final String sessionId) {
        synchronized (sessionMap) {
            return sessionMap.containsKey(sessionId);
        }
    }

    @Override
    public void removeAndCloseIfExists(final String sessionId) {
        final SharedReference<ImhotepSession> imhotepSession;
        synchronized (sessionMap) {
            final Session<E> session = sessionMap.remove(sessionId);
            if (session == null) {
                return;
            }
            imhotepSession = session.imhotepSession;
        }
        Closeables2.closeQuietly(imhotepSession, log);
    }

    @Override
    public void removeAndCloseIfExists(final String sessionId, Exception e) {
        final SharedReference<ImhotepSession> imhotepSession;
        synchronized (sessionMap) {
            final Session<E> session = sessionMap.remove(sessionId);
            if (session == null) {
                return;
            }
            imhotepSession = session.imhotepSession;
        }
        failureCauseMap.put(sessionId, e);
        Closeables2.closeQuietly(imhotepSession, log);
    }

    @Override
    public void setNumStats(final String sessionId, final int newNumStats) {
        final Session<E> session = internalGetSession(sessionId);
        session.numStats = newNumStats;
    }

    @Override
    public int getNumStats(final String sessionId) {
        final Session<E> session = internalGetSession(sessionId);
        return session.numStats;
    }

    @Override
    public Map<String, Long> getLastActionTimes() {
        final Map<String, Session<E>> sessionMap = cloneSessionMap();

        final Map<String, Long> ret = new HashMap<String, Long>();
        for (final Map.Entry<String, Session<E>> e : sessionMap.entrySet()) {
            ret.put(e.getKey(), e.getValue().lastActionTime);
        }
        return ret;
    }

    protected Map<String, Session<E>> cloneSessionMap() {
        final Map<String, Session<E>> clone;
        synchronized (sessionMap) {
            clone = new HashMap<String, Session<E>>(sessionMap);
        }
        return clone;
    }

    protected Session<E> internalGetSession(final String sessionId) {
        final Session<E> session;
        synchronized (sessionMap) {
            checkSession(sessionId);
            session = sessionMap.get(sessionId);
        }
        session.lastActionTime = System.currentTimeMillis();
        return session;
    }

    // do not call this method if you do not hold sessionMap's monitor
    private void checkSession(final String sessionId) {
        if (!sessionMap.containsKey(sessionId)) {
            final Exception e = failureCauseMap.get(sessionId);
            throw new IllegalArgumentException("there does not exist a session with id " + sessionId, e);
        }
    }

    protected static final class Session<E> {
        protected final SharedReference<ImhotepSession> imhotepSession;
        protected final E sessionState;
        protected final String username;
        protected final String ipAddress;
        protected final int clientVersion;
        protected final String dataset;

        private volatile int numStats;
        private volatile long lastActionTime;

        protected Session(
                ImhotepSession imhotepSession,
                E sessionState,
                String username,
                String ipAddress,
                int clientVersion,
                String dataset
        ) {
            this.imhotepSession = SharedReference.create(imhotepSession);
            this.sessionState = sessionState;
            this.username = username;
            this.ipAddress = ipAddress;
            this.clientVersion = clientVersion;
            this.dataset = dataset;

            lastActionTime = System.currentTimeMillis();
        }
    }

    @Export(name = "session-count", doc = "# of open sessions")
    public int getSessionCount() {
        synchronized (sessionMap) {
            return sessionMap.size();
        }
    }
}

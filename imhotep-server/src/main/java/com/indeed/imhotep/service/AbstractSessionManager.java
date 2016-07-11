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
 package com.indeed.imhotep.service;

import com.google.common.cache.CacheBuilder;
import com.indeed.util.core.io.Closeables2;
import com.indeed.util.core.reference.SharedReference;
import com.indeed.util.varexport.Export;
import com.indeed.imhotep.api.ImhotepSession;
import com.indeed.imhotep.service.ImhotepTooManySessionsException;

import org.apache.log4j.Logger;

import java.util.HashMap;
import java.util.Map;

/**
 * @author jplaisance
 */
public abstract class AbstractSessionManager<E> implements SessionManager<E> {

    private static final Logger log = Logger.getLogger(AbstractSessionManager.class);

    private static int MAX_SESSION_COUNT = 30;
    private static int MAX_SESSION_COUNT_PER_USER = 4;

    private final Map<String, Session<E>> sessionMap = new HashMap<String, Session<E>>();
    private final Map<String, Exception> failureCauseMap = CacheBuilder.newBuilder().maximumSize(200).<String, Exception>build().asMap();

    protected void addSession(String sessionId, Session<E> session) {
        synchronized (sessionMap) {
            if (sessionMap.containsKey(sessionId)) {
                throw new IllegalArgumentException("there already exists a session with id "+sessionId);
            } else if (sessionMap.size() >= MAX_SESSION_COUNT) {
                throw new ImhotepTooManySessionsException("This daemon has reached the maximum number of concurrent sessions: "+sessionMap.size());
            }
            int userSessionCount = 0;
            for (final Map.Entry<String, Session<E>> e : sessionMap.entrySet()) {
                if (e.getValue().getUsername().equals(session.getUsername())) {
                    userSessionCount += 1;
                }
            }
            if (userSessionCount >= MAX_SESSION_COUNT_PER_USER) {
                throw new ImhotepTooManySessionsException("This daemon has reached the maximum number of concurrent sessions per user: "+userSessionCount);
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
    public Map<String, LastActionTimeLimit> getLastActionTimes() {
        final Map<String, Session<E>> sessionMap = cloneSessionMap();

        final Map<String, LastActionTimeLimit> ret = new HashMap<String, LastActionTimeLimit>();
        for (final Map.Entry<String, Session<E>> e : sessionMap.entrySet()) {
            ret.put(e.getKey(), new LastActionTimeLimit(e.getValue().lastActionTime, e.getValue().getTimeout()));
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
        protected final long timeout;
        protected final long SESSION_TIMEOUT_DEFAULT = 30L * 60 * 1000;

        private volatile int numStats;
        private volatile long lastActionTime;

        protected Session(
                ImhotepSession imhotepSession,
                E sessionState,
                String username,
                String ipAddress,
                int clientVersion,
                String dataset,
                long timeout
        ) {
            this.imhotepSession = SharedReference.create(imhotepSession);
            this.sessionState = sessionState;
            this.username = username;
            this.ipAddress = ipAddress;
            this.clientVersion = clientVersion;
            this.dataset = dataset;
            if(timeout < 1) {
                this.timeout = SESSION_TIMEOUT_DEFAULT;
            } else {
                this.timeout = timeout;
            }


            lastActionTime = System.currentTimeMillis();
        }

        public long getTimeout() {
            return timeout;
        }

        public String getUsername() {
            return username;
        }
    }

    @Export(name = "session-count", doc = "# of open sessions")
    public int getSessionCount() {
        synchronized (sessionMap) {
            return sessionMap.size();
        }
    }
}

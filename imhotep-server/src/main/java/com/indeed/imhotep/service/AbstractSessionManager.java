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

import com.google.common.base.Strings;
import com.google.common.cache.CacheBuilder;
import com.indeed.imhotep.MemoryReservationContext;
import com.indeed.imhotep.exceptions.TooManySessionsException;
import com.indeed.imhotep.exceptions.UserSessionCountLimitExceededException;
import com.indeed.imhotep.local.MTImhotepLocalMultiSession;
import com.indeed.util.core.io.Closeables2;
import com.indeed.util.core.reference.SharedReference;
import com.indeed.util.varexport.Export;
import org.apache.log4j.Logger;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * @author jplaisance
 */
public abstract class AbstractSessionManager<E> implements SessionManager<E> {

    protected static final Logger log = Logger.getLogger(AbstractSessionManager.class);


    private final Map<String, Session<E>> sessionMap = new HashMap<>();
    private final Map<String, Exception> failureCauseMap = CacheBuilder.newBuilder().maximumSize(200).<String, Exception>build().asMap();
    private final int maxSessionsTotal;
    private final int maxSessionsPerUser;

    public AbstractSessionManager(final int maxSessionsTotal, final int maxSessionsPerUser) {
        this.maxSessionsTotal = maxSessionsTotal;
        this.maxSessionsPerUser = maxSessionsPerUser;
    }

    protected void addSession(final String sessionId, final Session<E> session) {
        synchronized (sessionMap) {
            if (sessionMap.containsKey(sessionId)) {
                throw new IllegalArgumentException("there already exists a session with id "+sessionId);
            }
            if (sessionMap.size() >= maxSessionsTotal) {
                throw new TooManySessionsException("Imhotep daemon has reached the maximum number of concurrent sessions: "+sessionMap.size());
            }

            if (!Strings.isNullOrEmpty(session.username) && !session.username.equals(session.clientName)) {
                int userSessionCount = 0;
                for (final Map.Entry<String, Session<E>> e : sessionMap.entrySet()) {
                    if (e.getValue().getUsername().equals(session.getUsername())) {
                        userSessionCount += 1;
                    }
                }
                if (userSessionCount >= maxSessionsPerUser) {
                    throw new UserSessionCountLimitExceededException("Imhotep daemon has reached the maximum number of concurrent sessions per user: " + userSessionCount);
                }
            }

            sessionMap.put(sessionId, session);
        }
    }

    @Override
    public SharedReference<MTImhotepLocalMultiSession> getSession(final String sessionId) {
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
        final SharedReference<MTImhotepLocalMultiSession> imhotepSession;
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
    public void removeAndCloseIfExists(final String sessionId, final Exception e) {
        final SharedReference<MTImhotepLocalMultiSession> imhotepSession;
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
    public Map<String, LastActionTimeLimit> getLastActionTimes() {
        final Map<String, Session<E>> sessionMap = cloneSessionMap();

        final Map<String, LastActionTimeLimit> ret = new HashMap<>();
        for (final Map.Entry<String, Session<E>> e : sessionMap.entrySet()) {
            ret.put(e.getKey(), new LastActionTimeLimit(e.getValue().lastActionTime, e.getValue().getTimeout()));
        }
        return ret;
    }

    protected Map<String, Session<E>> cloneSessionMap() {
        final Map<String, Session<E>> clone;
        synchronized (sessionMap) {
            clone = new HashMap<>(sessionMap);
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
        protected final SharedReference<MTImhotepLocalMultiSession> imhotepSession;
        protected final E sessionState;
        protected final String username;
        protected final String clientName;
        protected final String ipAddress;
        protected final int clientVersion;
        protected final String dataset;
        protected final MemoryReservationContext sessionMemoryContext;
        protected final long timeout;
        protected final long creationTime;
        protected final long SESSION_TIMEOUT_DEFAULT = 30L * 60 * 1000;

        private volatile long lastActionTime;

        protected Session(
                final MTImhotepLocalMultiSession imhotepSession,
                final E sessionState,
                final String username,
                final String clientName,
                final String ipAddress,
                final int clientVersion,
                final String dataset,
                final long timeout,
                final MemoryReservationContext sessionMemoryContext) {
            this.imhotepSession = SharedReference.create(imhotepSession);
            this.sessionState = sessionState;
            this.username = username;
            this.clientName = clientName;
            this.ipAddress = ipAddress;
            this.clientVersion = clientVersion;
            this.dataset = dataset;
            this.sessionMemoryContext = sessionMemoryContext;
            if(timeout < 1) {
                this.timeout = SESSION_TIMEOUT_DEFAULT;
            } else {
                this.timeout = timeout;
            }

            creationTime = System.currentTimeMillis();

            lastActionTime = creationTime;
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

    public int getActiveUserCount() {
        final Map<String, Session<E>> sessionMap = cloneSessionMap();
        final Set<String> uniqueUsernames = new HashSet<>();
        for (Session<E> session : sessionMap.values()) {
            uniqueUsernames.add(session.username);
        }
        return uniqueUsernames.size();
    }

}

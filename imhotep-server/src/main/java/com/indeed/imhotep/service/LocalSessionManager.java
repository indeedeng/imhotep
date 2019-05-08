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
import com.indeed.imhotep.ImhotepStatusDump;
import com.indeed.imhotep.MemoryReservationContext;
import com.indeed.imhotep.ShardDir;
import com.indeed.imhotep.exceptions.InvalidSessionException;
import com.indeed.imhotep.exceptions.TooManySessionsException;
import com.indeed.imhotep.exceptions.UserSessionCountLimitExceededException;
import com.indeed.imhotep.local.MTImhotepLocalMultiSession;
import com.indeed.util.core.io.Closeables2;
import com.indeed.util.core.reference.SharedReference;
import com.indeed.util.core.threads.NamedThreadFactory;
import com.indeed.util.varexport.Export;
import org.apache.log4j.Logger;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * @author jsgroth
 *
 * this class is thread-safe
 */
public final class LocalSessionManager implements SessionManager {
    protected static final Logger log = Logger.getLogger(LocalSessionManager.class);
    
    private final Map<String, Session> sessionMap = new HashMap<>();
    private final Map<String, Exception> failureCauseMap = CacheBuilder.newBuilder().maximumSize(200).<String, Exception>build().asMap();
    private final int maxSessionsTotal;
    private final int maxSessionsPerUser;
    private final MetricStatsEmitter statsEmitter;

    private static final int REPORTING_FREQUENCY_MILLIS = 100;

    public LocalSessionManager(final MetricStatsEmitter statsEmitter, final int maxSessionsTotal, final int maxSessionsPerUser) {
        this.maxSessionsTotal = maxSessionsTotal;
        this.maxSessionsPerUser = maxSessionsPerUser;
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
            final Map<Path, CachedFlamdexReaderReference> flamdexes,
            final String username,
            final String clientName,
            final String ipAddress,
            final int clientVersion,
            final String dataset,
            final long sessionTimeout,
            final byte priority,
            final MemoryReservationContext sessionMemoryContext) {
        final Session session = new Session(imhotepSession, flamdexes, username, clientName, ipAddress, clientVersion, dataset, sessionTimeout, priority, sessionMemoryContext);
        addSession(sessionId, session);
    }

    protected void addSession(final String sessionId, final Session session) {
        synchronized (sessionMap) {
            if (sessionMap.containsKey(sessionId)) {
                throw new IllegalArgumentException("there already exists a session with id "+sessionId);
            }
            if (sessionMap.size() >= maxSessionsTotal) {
                throw new TooManySessionsException("Imhotep daemon has reached the maximum number of concurrent sessions: "+sessionMap.size());
            }

            if (!Strings.isNullOrEmpty(session.username) && !session.username.equals(session.clientName)) {
                int userSessionCount = 0;
                for (final Map.Entry<String, Session> e : sessionMap.entrySet()) {
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
        final Session session = internalGetSession(sessionId);
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
        removeAndCloseIfExists(sessionId, null);
    }

    @Override
    public void removeAndCloseIfExists(final String sessionId, final Exception e) {
        final SharedReference<MTImhotepLocalMultiSession> imhotepSession;
        final Map<Path, CachedFlamdexReaderReference> shardToFlamdexReader;
        synchronized (sessionMap) {
            final Session session = sessionMap.remove(sessionId);
            if (session == null) {
                return;
            }
            imhotepSession = session.imhotepSession;
            shardToFlamdexReader = session.shardToFlamdexReader;
        }
        if(e != null) {
            failureCauseMap.put(sessionId, e);
        }
        Closeables2.closeQuietly(imhotepSession, log);
        // have to close our copy of the shared references
        Closeables2.closeAll(shardToFlamdexReader.values(), log);
    }

    @Override
    public Map<String, SessionManager.LastActionTimeLimit> getLastActionTimes() {
        final Map<String, Session> sessionMap = cloneSessionMap();

        final Map<String, SessionManager.LastActionTimeLimit> ret = new HashMap<>();
        for (final Map.Entry<String, Session> e : sessionMap.entrySet()) {
            ret.put(e.getKey(), new SessionManager.LastActionTimeLimit(e.getValue().lastActionTime, e.getValue().getTimeout()));
        }
        return ret;
    }

    protected Map<String, Session> cloneSessionMap() {
        final Map<String, Session> clone;
        synchronized (sessionMap) {
            clone = new HashMap<>(sessionMap);
        }
        return clone;
    }

    protected Session internalGetSession(final String sessionId) {
        final Session session;
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
            throw new InvalidSessionException("there does not exist a session with id " + sessionId, e);
        }
    }

    protected static final class Session {
        protected final SharedReference<MTImhotepLocalMultiSession> imhotepSession;
        protected final Map<Path, CachedFlamdexReaderReference> shardToFlamdexReader;
        protected final String username;
        protected final String clientName;
        protected final String ipAddress;
        protected final int clientVersion;
        protected final String dataset;
        protected final byte priority;
        protected final MemoryReservationContext sessionMemoryContext;
        protected final long timeout;
        protected final long creationTime;
        protected final long SESSION_TIMEOUT_DEFAULT = 30L * 60 * 1000;

        private volatile long lastActionTime;

        protected Session(
                final MTImhotepLocalMultiSession imhotepSession,
                final Map<Path, CachedFlamdexReaderReference> shardToFlamdexReader,
                final String username,
                final String clientName,
                final String ipAddress,
                final int clientVersion,
                final String dataset,
                final long timeout,
                final byte priority,
                final MemoryReservationContext sessionMemoryContext) {
            this.imhotepSession = SharedReference.create(imhotepSession);
            this.shardToFlamdexReader = shardToFlamdexReader;
            this.username = username;
            this.clientName = clientName;
            this.ipAddress = ipAddress;
            this.clientVersion = clientVersion;
            this.dataset = dataset;
            this.priority = priority;
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
        final Map<String, Session> sessionMap = cloneSessionMap();
        final Set<String> uniqueUsernames = new HashSet<>();
        for (Session session : sessionMap.values()) {
            uniqueUsernames.add(session.username);
        }
        return uniqueUsernames.size();
    }

    public List<String> getShardsForSession(final String sessionId) {
        final Session session = internalGetSession(sessionId);
        final List<String> ret = new ArrayList<>(session.shardToFlamdexReader.size());
        for (final Path path : session.shardToFlamdexReader.keySet()) {
            ret.add(path.toString());
        }
        return ret;
    }

    public List<ImhotepStatusDump.SessionDump> getSessionDump() {
        final Map<String, Session> clone = cloneSessionMap();

        final List<ImhotepStatusDump.SessionDump> openSessions = new ArrayList<>(clone.size());
        for (final String sessionId : clone.keySet()) {
            final Session session = clone.get(sessionId);
            final List<ImhotepStatusDump.ShardDump> openShards = new ArrayList<>();
            for (final Map.Entry<Path, CachedFlamdexReaderReference> entry : session.shardToFlamdexReader.entrySet()) {
                final ShardDir shard = new ShardDir(entry.getKey());
                openShards.add(new ImhotepStatusDump.ShardDump(shard.getId(), shard.getDataset(), entry.getValue().getNumDocs(), entry.getValue().getMetricDump()));
            }
            openSessions.add(new ImhotepStatusDump.SessionDump(sessionId, session.dataset, "", session.username, session.clientName,
                    session.ipAddress, session.clientVersion, session.creationTime, openShards,
                    session.sessionMemoryContext.usedMemory(), session.sessionMemoryContext.getGlobalMaxUsedMemory(), session.priority));
        }
        return openSessions;
    }
}

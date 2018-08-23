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

import com.indeed.imhotep.MemoryReservationContext;
import com.indeed.imhotep.local.MTImhotepLocalMultiSession;
import com.indeed.util.core.reference.SharedReference;

import java.util.Map;

/**
 * @author jplaisance
 */
public interface SessionManager {

    void addSession(
            String sessionId,
            MTImhotepLocalMultiSession imhotepSession,
            Map<ShardId, CachedFlamdexReaderReference> sessionState,
            String username,
            String clientName,
            String ipAddress,
            int clientVersion,
            String dataset,
            long sessionTimeout,
            MemoryReservationContext sessionMemoryContext);

    SharedReference<MTImhotepLocalMultiSession> getSession(String sessionId);

    boolean sessionIsValid(String sessionId);

    void removeAndCloseIfExists(String sessionId);
    void removeAndCloseIfExists(String sessionId, Exception e);

    Map<String, LastActionTimeLimit> getLastActionTimes();

    public class LastActionTimeLimit {
        private final long lastActionTime;
        private final long sessionTimeoutDuration;

        public LastActionTimeLimit(final long lastActionTime, final long sessionTimeoutDuration) {
            this.lastActionTime = lastActionTime;
            this.sessionTimeoutDuration = sessionTimeoutDuration;
        }

        public long getLastActionTime() {
            return lastActionTime;
        }

        public long getSessionTimeoutDuration() {
            return sessionTimeoutDuration;
        }
    }


}

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
            String dataset,
            long sessionTimeout
    );

    SharedReference<ImhotepSession> getSession(String sessionId);

    boolean sessionIsValid(String sessionId);

    void removeAndCloseIfExists(String sessionId);
    void removeAndCloseIfExists(String sessionId, Exception e);

    void setNumStats(String sessionId, int newNumStats);

    int getNumStats(String sessionId);

    Map<String, LastActionTimeLimit> getLastActionTimes();

    public class LastActionTimeLimit {
        final private long lastActionTime;
        final private long sessionTimeoutDuration;

        public LastActionTimeLimit(long lastActionTime, long sessionTimeoutDuration) {
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

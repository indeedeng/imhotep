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

import com.indeed.imhotep.MemoryReservationContext;
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
            final String clientName,
            final String ipAddress,
            final int clientVersion,
            final String dataset,
            final long sessionTimeout,
            MemoryReservationContext sessionMemoryContext) {
        final Session<List<String>> session = new Session<List<String>>(imhotepSession, sessionShardIds, username, clientName, ipAddress, clientVersion, dataset, sessionTimeout, sessionMemoryContext);
        addSession(sessionId, session);
    }

    public List<String> getShardIdsForSession(String sessionId) {
        return internalGetSession(sessionId).sessionState;
    }
}

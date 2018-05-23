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

import com.indeed.imhotep.Instrumentation;
import com.indeed.imhotep.Instrumentation.Keys;
import com.indeed.imhotep.protobuf.ImhotepRequest;
import com.indeed.imhotep.protobuf.ImhotepResponse;

class DaemonEvents {

    static class HandleRequestEvent extends Instrumentation.Event {

        public static final String NAME = HandleRequestEvent.class.getSimpleName();

        public HandleRequestEvent(final String          name,
                                  final ImhotepRequest  protoRequest,
                                  final ImhotepResponse protoResponse,
                                  final String          sourceAddr,
                                  final String          targetAddr,
                                  final long            beginTmMillis,
                                  final long            elapsedTmMillis) {
            super(name);
            final String sessionId = protoRequest.getSessionId();
            getProperties().put(Keys.BEGIN_TIME_MILLIS,   beginTmMillis);
            getProperties().put(Keys.ELAPSED_TIME_MILLIS, elapsedTmMillis);
            getProperties().put(Keys.REQUEST_SIZE,        protoRequest.getSerializedSize());
            getProperties().put(Keys.REQUEST_TYPE,        protoRequest.getRequestType().toString());
            getProperties().put(Keys.SESSION_ID,          protoRequest.getSessionId());
            getProperties().put(Keys.SOURCE_ADDR,         sourceAddr);
            getProperties().put(Keys.TARGET_ADDR,         targetAddr);
            if (protoResponse != null) {
                getProperties().put(Keys.RESPONSE_SIZE, protoResponse.getSerializedSize());
            }
            getProperties().put(Keys.USERNAME,            protoRequest.getUsername());
        }

        public HandleRequestEvent(final ImhotepRequest  protoRequest,
                                  final ImhotepResponse protoResponse,
                                  final String          sourceAddr,
                                  final String          targetAddr,
                                  final long            beginTmMillis,
                                  final long            elapsedTmMillis) {
            this(NAME, protoRequest, protoResponse,
                 sourceAddr, targetAddr,
                 beginTmMillis, elapsedTmMillis);
        }
    }

    static final class OpenSessionEvent extends HandleRequestEvent {

        public static final String NAME = OpenSessionEvent.class.getSimpleName();

        public OpenSessionEvent(final ImhotepRequest  protoRequest,
                                final ImhotepResponse protoResponse,
                                final String          sourceAddr,
                                final String          targetAddr,
                                final long            beginTmMillis,
                                final long            elapsedTmMillis) {
            super(NAME, protoRequest, protoResponse,
                  sourceAddr, targetAddr,
                  beginTmMillis, elapsedTmMillis);

            getProperties().put(Keys.DATASET,            protoRequest.getDataset());
            getProperties().put(Keys.SHARD_REQUEST_LIST, protoRequest.getShardRequestList());
            getProperties().put(Keys.USERNAME,           protoRequest.getUsername());
        }
    }
}

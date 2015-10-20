package com.indeed.imhotep.service;

import com.indeed.imhotep.Instrumentation;
import com.indeed.imhotep.Instrumentation.Keys;
import com.indeed.imhotep.protobuf.ImhotepRequest;
import com.indeed.imhotep.protobuf.ImhotepResponse;

class DaemonEvents {

    static final class OpenSessionEvent extends Instrumentation.Event {

        public static final String NAME = OpenSessionEvent.class.getSimpleName();

        public OpenSessionEvent(final ImhotepRequest protoRequest,
                                final String         sourceAddr,
                                final String         targetAddr) {
            super(NAME);
            getProperties().put(Keys.DATASET,            protoRequest.getDataset());
            getProperties().put(Keys.REQUEST_SIZE,       protoRequest.getSerializedSize());
            getProperties().put(Keys.SESSION_ID,         protoRequest.getSessionId());
            getProperties().put(Keys.SHARD_REQUEST_LIST, protoRequest.getShardRequestList());
            getProperties().put(Keys.SOURCE_ADDR,        sourceAddr);
            getProperties().put(Keys.TARGET_ADDR,        targetAddr);
            getProperties().put(Keys.USERNAME,           protoRequest.getUsername());
            getProperties().put(Keys.USE_NATIVE_FTGS,    protoRequest.getUseNativeFtgs());
        }
    }

    static final class CloseSessionEvent extends Instrumentation.Event {

        public static final String NAME = CloseSessionEvent.class.getSimpleName();

        public CloseSessionEvent(final ImhotepRequest protoRequest,
                                 final String         sourceAddr,
                                 final String         targetAddr) {
            super(NAME);
            final String sessionId = protoRequest.getSessionId();
            getProperties().put(Keys.REQUEST_SIZE, protoRequest.getSerializedSize());
            getProperties().put(Keys.SESSION_ID,   protoRequest.getSessionId());
            getProperties().put(Keys.SOURCE_ADDR,  sourceAddr);
            getProperties().put(Keys.TARGET_ADDR,  targetAddr);
        }
    }

    static final class HandleRequestEvent extends Instrumentation.Event {

        public static final String NAME = HandleRequestEvent.class.getSimpleName();

        public HandleRequestEvent(final ImhotepRequest  protoRequest,
                                  final ImhotepResponse protoResponse,
                                  final String          sourceAddr,
                                  final String          targetAddr,
                                  final long            elapsedTmNanos) {
            super(NAME);
            final String sessionId = protoRequest.getSessionId();
            getProperties().put(Keys.ELAPSED_TM_NANOS, elapsedTmNanos);
            getProperties().put(Keys.REQUEST_SIZE,     protoRequest.getSerializedSize());
            getProperties().put(Keys.REQUEST_TYPE,     protoRequest.getRequestType().toString());
            getProperties().put(Keys.SESSION_ID,       protoRequest.getSessionId());
            getProperties().put(Keys.SOURCE_ADDR,      sourceAddr);
            getProperties().put(Keys.TARGET_ADDR,      targetAddr);
            if (protoResponse != null) {
                getProperties().put(Keys.RESPONSE_SIZE, protoResponse.getSerializedSize());
            }
        }
    }
}

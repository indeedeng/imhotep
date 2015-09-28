package com.indeed.imhotep.service;

import com.indeed.imhotep.Instrumentation;
import com.indeed.imhotep.protobuf.ImhotepRequest;

class DaemonEvents {

    public static final String DATASET            = "dataset";
    public static final String SESSION_ID         = "sessionId";
    public static final String SHARD_REQUEST_LIST = "shardRequestList";
    public static final String USERNAME           = "username";
    public static final String USE_NATIVE_FTGS    = "useNativeFTGS";

    static final class OpenSession extends Instrumentation.Event {

        public static final String NAME = OpenSession.class.getSimpleName();

        public OpenSession(final ImhotepRequest protoRequest) {
            super(NAME);
            getProperties().put(DATASET,            protoRequest.getDataset());
            getProperties().put(SHARD_REQUEST_LIST, protoRequest.getShardRequestList());
            getProperties().put(SESSION_ID,         protoRequest.getSessionId());
            getProperties().put(USERNAME,           protoRequest.getUsername());
            getProperties().put(USE_NATIVE_FTGS,    protoRequest.getUseNativeFtgs());
        }
    }

    static final class CloseSession extends Instrumentation.Event {

        public static final String NAME = CloseSession.class.getSimpleName();

        public CloseSession(final ImhotepRequest protoRequest) {
            super(NAME);
            final String sessionId = protoRequest.getSessionId();
            getProperties().put(SESSION_ID, protoRequest.getSessionId());
        }
    }
}

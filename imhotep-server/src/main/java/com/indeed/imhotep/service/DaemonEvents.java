package com.indeed.imhotep.service;

import java.util.HashMap;

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
            super(NAME, new HashMap<Object, Object>());
            getProperties().put(DATASET,            protoRequest.getDataset());
            getProperties().put(SHARD_REQUEST_LIST, protoRequest.getShardRequestList());
            getProperties().put(SESSION_ID,         protoRequest.getSessionId());
            getProperties().put(USERNAME,           protoRequest.getUsername());
            getProperties().put(USE_NATIVE_FTGS,    protoRequest.getUseNativeFtgs());
        }
    }
}

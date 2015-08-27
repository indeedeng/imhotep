package com.indeed.imhotep.service;

import java.util.HashMap;

import com.indeed.imhotep.protobuf.ImhotepRequest;

class OpenSessionEvent extends Instrumentation.Event {

    public static final String NAME = OpenSessionEvent.class.getName();

    public static final String DATASET    = "dataset";
    public static final String SESSION_ID = "sessionId";
    public static final String USERNAME   = "username";
    // etc...

    public OpenSessionEvent(final ImhotepRequest protoRequest) {
        super(NAME, new HashMap<Object, Object>());
        getProperties().put(DATASET,    protoRequest.getDataset());
        getProperties().put(SESSION_ID, protoRequest.getSessionId());
        getProperties().put(USERNAME,   protoRequest.getUsername());
        // etc...
    }
}

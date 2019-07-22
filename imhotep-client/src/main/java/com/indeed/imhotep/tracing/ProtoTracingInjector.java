package com.indeed.imhotep.tracing;

import com.indeed.imhotep.protobuf.TracingMap;
import io.opentracing.propagation.TextMap;

import java.util.Iterator;
import java.util.Map;

public class ProtoTracingInjector implements TextMap {
    private final TracingMap.Builder tracingBuilder = TracingMap.newBuilder();

    @Override
    public Iterator<Map.Entry<String, String>> iterator() {
        throw new UnsupportedOperationException("This is a write-only adapter");
    }

    @Override
    public void put(final String key, final String value) {
        tracingBuilder.addKeyValues(TracingMap.KeyValueString.newBuilder().setKey(key).setValue(value).build());
    }

    public TracingMap extract() {
        return tracingBuilder.build();
    }
}

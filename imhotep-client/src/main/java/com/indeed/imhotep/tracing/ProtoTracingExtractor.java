package com.indeed.imhotep.tracing;

import com.google.common.collect.Iterators;
import com.indeed.imhotep.protobuf.ImhotepRequest;
import io.opentracing.propagation.TextMap;

import java.util.AbstractMap;
import java.util.Iterator;
import java.util.Map;

public class ProtoTracingExtractor implements TextMap {
    private final ImhotepRequest request;

    public ProtoTracingExtractor(final ImhotepRequest request) {
        this.request = request;
    }

    @Override
    public Iterator<Map.Entry<String, String>> iterator() {
        return Iterators.transform(
                request.getTracingInfo().getKeyValuesList().iterator(),
                x -> new AbstractMap.SimpleEntry<>(x.getKey(), x.getValue())
        );
    }

    @Override
    public void put(final String key, final String value) {
        throw new UnsupportedOperationException("You need to implement this");
    }
}

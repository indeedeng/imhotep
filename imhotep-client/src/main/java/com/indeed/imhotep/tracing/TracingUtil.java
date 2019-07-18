package com.indeed.imhotep.tracing;

import io.opentracing.ActiveSpan;
import io.opentracing.NoopTracer;
import io.opentracing.NoopTracerFactory;
import io.opentracing.Tracer;
import io.opentracing.util.GlobalTracer;

public class TracingUtil {
    private static final NoopTracer NOOP_TRACER = NoopTracerFactory.create();

    public static Tracer tracerIfInActiveSpan() {
        final Tracer tracer = GlobalTracer.get();
        final ActiveSpan activeSpan = tracer.activeSpan();
        if (activeSpan != null) {
            return tracer;
        }
        return NOOP_TRACER;
    }

    public static Tracer tracerIfEnabled(final boolean enabled) {
        return enabled ? GlobalTracer.get() : NOOP_TRACER;
    }

    public static boolean isTracingActive() {
        return GlobalTracer.get().activeSpan() != null;
    }
}

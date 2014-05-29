package com.indeed.imhotep.service;

import com.indeed.flamdex.api.FlamdexOutOfMemoryException;
import com.indeed.flamdex.api.IntValueLookup;
import com.indeed.imhotep.ImhotepStatusDump;

import java.io.Closeable;
import java.util.List;
import java.util.Set;

/**
 * @author jplaisance
 */
public interface MetricCache extends Closeable {

    public IntValueLookup getMetric(String metric) throws FlamdexOutOfMemoryException;

    public List<ImhotepStatusDump.MetricDump> getMetricDump();

    public Set<String> getLoadedMetrics();

    public void close();
}

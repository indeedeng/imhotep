package com.indeed.imhotep.service;

import com.indeed.flamdex.api.IntValueLookup;
import com.indeed.flamdex.api.RawFlamdexReader;
import com.indeed.flamdex.api.RawStringTermDocIterator;
import com.indeed.flamdex.api.RawStringTermIterator;
import com.indeed.imhotep.ImhotepMemoryCache;
import com.indeed.imhotep.MemoryReservationContext;
import com.indeed.imhotep.MetricKey;
import org.apache.log4j.Logger;

import javax.annotation.Nullable;
import java.io.Closeable;

/**
 * @author jplaisance
 */
public final class RawCachedFlamdexReader extends CachedFlamdexReader implements RawFlamdexReader {

    private static final Logger log = Logger.getLogger(RawCachedFlamdexReader.class);

    public RawCachedFlamdexReader(
            final MemoryReservationContext memory,
            final RawFlamdexReader wrapped,
            final @Nullable Closeable readLockRef,
            final String indexName,
            final String shardName,
            final ImhotepMemoryCache<MetricKey, IntValueLookup> freeCache
    ) {
        super(memory, wrapped, readLockRef, indexName, shardName, freeCache);
    }

    @Override
    public RawStringTermIterator getStringTermIterator(final String field) {
        return (RawStringTermIterator)super.getStringTermIterator(field);
    }

    @Override
    public RawStringTermDocIterator getStringTermDocIterator(final String field) {
        return (RawStringTermDocIterator)super.getStringTermDocIterator(field);
    }
}

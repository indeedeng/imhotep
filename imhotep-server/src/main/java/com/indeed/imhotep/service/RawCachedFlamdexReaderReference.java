package com.indeed.imhotep.service;

import com.indeed.util.core.reference.SharedReference;
import com.indeed.flamdex.api.RawFlamdexReader;
import com.indeed.flamdex.api.RawStringTermDocIterator;
import com.indeed.flamdex.api.RawStringTermIterator;
import org.apache.log4j.Logger;

/**
 * @author jplaisance
 */
public final class RawCachedFlamdexReaderReference extends CachedFlamdexReaderReference implements RawFlamdexReader {

    private static final Logger log = Logger.getLogger(RawCachedFlamdexReaderReference.class);

    private final RawFlamdexReader reader;

    public RawCachedFlamdexReaderReference(final SharedReference<? extends RawCachedFlamdexReader> reference) {
        super(reference);
        this.reader = reference.get();
    }

    @Override
    public RawStringTermIterator getStringTermIterator(final String field) {
        return reader.getStringTermIterator(field);
    }

    @Override
    public RawStringTermDocIterator getStringTermDocIterator(final String field) {
        return reader.getStringTermDocIterator(field);
    }
}

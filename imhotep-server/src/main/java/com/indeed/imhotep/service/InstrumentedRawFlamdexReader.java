package com.indeed.imhotep.service;

import com.indeed.flamdex.api.RawFlamdexReader;
import com.indeed.flamdex.api.RawStringTermDocIterator;
import com.indeed.flamdex.api.RawStringTermIterator;

public class InstrumentedRawFlamdexReader extends InstrumentedFlamdexReader
        implements RawFlamdexReader {

    public InstrumentedRawFlamdexReader(final RawFlamdexReader reader) {
        super(reader);
    }

    @Override
    public RawStringTermIterator getStringTermIterator(final String field) {
        return (RawStringTermIterator)super.getStringTermIterator(field);
    }

    @Override
    public RawStringTermDocIterator getStringTermDocIterator(final String field) {
        return (RawStringTermDocIterator) super.getStringTermDocIterator(field);
    }
}

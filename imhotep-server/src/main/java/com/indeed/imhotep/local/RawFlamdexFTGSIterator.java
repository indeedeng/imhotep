package com.indeed.imhotep.local;

import com.indeed.util.core.reference.SharedReference;
import com.indeed.flamdex.api.FlamdexReader;
import com.indeed.flamdex.api.RawStringTermDocIterator;
import com.indeed.imhotep.api.RawFTGSIterator;

final class RawFlamdexFTGSIterator extends FlamdexFTGSIterator implements RawFTGSIterator {

    public RawFlamdexFTGSIterator(ImhotepLocalSession imhotepLocalSession, SharedReference<FlamdexReader> flamdexReader, String[] intFields, String[] stringFields) {
        super(imhotepLocalSession, flamdexReader, intFields, stringFields);
    }

    @Override
    public final byte[] termStringBytes() {
        return ((RawStringTermDocIterator)stringTermDocIterator).termStringBytes();
    }

    @Override
    public final int termStringLength() {
        return ((RawStringTermDocIterator)stringTermDocIterator).termStringLength();
    }
}
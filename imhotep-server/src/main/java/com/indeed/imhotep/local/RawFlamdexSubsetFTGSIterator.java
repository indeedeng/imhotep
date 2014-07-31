package com.indeed.imhotep.local;

import com.indeed.flamdex.api.FlamdexReader;
import com.indeed.flamdex.api.RawStringTermIterator;
import com.indeed.imhotep.api.RawFTGSIterator;
import com.indeed.util.core.reference.SharedReference;
import org.apache.log4j.Logger;

import java.util.Map;

/**
 * @author jplaisance
 */
public final class RawFlamdexSubsetFTGSIterator extends FlamdexSubsetFTGSIterator implements RawFTGSIterator {
    private static final Logger log = Logger.getLogger(RawFlamdexSubsetFTGSIterator.class);

    public RawFlamdexSubsetFTGSIterator(ImhotepLocalSession imhotepLocalSession, SharedReference<FlamdexReader> flamdexReader, Map<String, long[]> intFieldToTerms, Map<String, String[]> stringFieldToTerms) {
        super(imhotepLocalSession, flamdexReader, intFieldToTerms, stringFieldToTerms);
    }

    @Override
    public final byte[] termStringBytes() {
        return ((RawStringTermIterator)stringTermIterator).termStringBytes();
    }

    @Override
    public final int termStringLength() {
        return ((RawStringTermIterator)stringTermIterator).termStringLength();
    }
}

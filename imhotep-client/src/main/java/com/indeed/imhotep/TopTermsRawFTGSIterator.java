package com.indeed.imhotep;

import com.google.common.base.Charsets;
import com.indeed.imhotep.api.RawFTGSIterator;

/**
 * @author kenh
 */

public class TopTermsRawFTGSIterator extends TopTermsFTGSIterator implements RawFTGSIterator {
    public TopTermsRawFTGSIterator(final FTGSIteratorUtil.TopTermsStatsByField topTermFTGS) {
        super(topTermFTGS);
    }

    @Override
    public byte[] termStringBytes() {
        return termStringVal().getBytes(Charsets.UTF_8);
    }

    @Override
    public int termStringLength() {
        return termStringBytes().length;
    }
}

/*
 * Copyright (C) 2014 Indeed Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
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

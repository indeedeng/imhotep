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
 package com.indeed.imhotep.service;

import com.indeed.util.core.reference.SharedReference;
import com.indeed.flamdex.api.RawFlamdexReader;
import com.indeed.flamdex.api.RawStringTermDocIterator;
import com.indeed.flamdex.api.RawStringTermIterator;
import org.apache.log4j.Logger;

/**
 * @author jplaisance
 */
public final class RawCachedFlamdexReaderReference
    extends CachedFlamdexReaderReference
    implements RawFlamdexReader {

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

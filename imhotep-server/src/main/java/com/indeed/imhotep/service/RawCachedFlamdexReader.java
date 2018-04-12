/*
 * Copyright (C) 2018 Indeed Inc.
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

import com.indeed.flamdex.api.IntValueLookup;
import com.indeed.flamdex.api.RawFlamdexReader;
import com.indeed.flamdex.api.RawStringTermDocIterator;
import com.indeed.flamdex.api.RawStringTermIterator;
import com.indeed.imhotep.ImhotepMemoryCache;
import com.indeed.imhotep.MemoryReservationContext;
import com.indeed.imhotep.MetricKey;
import org.apache.log4j.Logger;

/**
 * @author jplaisance
 */
public final class RawCachedFlamdexReader extends CachedFlamdexReader implements RawFlamdexReader {

    private static final Logger log = Logger.getLogger(RawCachedFlamdexReader.class);

    public RawCachedFlamdexReader(
            final MemoryReservationContext memory,
            final RawFlamdexReader wrapped,
            final String indexName,
            final String shardName,
            final ImhotepMemoryCache<MetricKey, IntValueLookup> freeCache
    ) {
        super(memory, wrapped, indexName, shardName, freeCache);
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

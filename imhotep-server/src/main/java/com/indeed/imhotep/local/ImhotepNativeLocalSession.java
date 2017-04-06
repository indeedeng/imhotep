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
import com.indeed.flamdex.simple.SimpleFlamdexReader;
import com.indeed.imhotep.GroupMultiRemapRule;
import com.indeed.imhotep.GroupStatsDummyIterator;
import com.indeed.imhotep.ImhotepMemoryPool;
import com.indeed.imhotep.MemoryReservationContext;
import com.indeed.imhotep.api.GroupStatsIterator;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.imhotep.service.CachedFlamdexReader;
import com.indeed.imhotep.service.RawCachedFlamdexReaderReference;
import org.apache.log4j.Logger;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public class ImhotepNativeLocalSession extends ImhotepLocalSession {

    static final Logger log = Logger.getLogger(ImhotepNativeLocalSession.class);

    private MultiCache multiCache;
    private boolean    rebuildMultiCache = true;

    private NativeShard nativeShard = null;

    /* !@# Blech! Egregious hack to access actual SFR wrapped within
        FlamdexReader passed in. */
    private SimpleFlamdexReader simpleFlamdexReader;

    public ImhotepNativeLocalSession(final FlamdexReader flamdexReader)
        throws ImhotepOutOfMemoryException {
        this(flamdexReader,
             new MemoryReservationContext(new ImhotepMemoryPool(Long.MAX_VALUE)), null);
    }

    public ImhotepNativeLocalSession(final FlamdexReader flamdexReader,
                                     final MemoryReservationContext memory,
                                     AtomicLong tempFileSizeBytesLeft)
        throws ImhotepOutOfMemoryException {

        super(flamdexReader, memory, tempFileSizeBytesLeft);

        /* !@# egregious hack to work around the fact that while we know damn
            well we have access to a SimpleFlamdexReader in this context, it's
            buried beneath wrappers of wrappers. */
        if (flamdexReader instanceof SimpleFlamdexReader) {
            this.simpleFlamdexReader = (SimpleFlamdexReader) flamdexReader;
        }
        else {
            RawCachedFlamdexReaderReference rcfrr = (RawCachedFlamdexReaderReference) flamdexReader;
            CachedFlamdexReader             cfr   = (CachedFlamdexReader) rcfrr.getReader();
            this.simpleFlamdexReader = (SimpleFlamdexReader) cfr.getWrapped();
        }

        this.statLookup.addObserver(new StatLookup.Observer() {
                public void onChange(final StatLookup statLookup, final int index) {
                    ImhotepNativeLocalSession.this.rebuildMultiCache = true;
                }
            });
    }

    public MultiCache buildMultiCache(final MultiCacheConfig config) {
        if (rebuildMultiCache) {
            if (multiCache != null) {
                multiCache.close();
            }
            multiCache = new MultiCache(this, (int)getNumDocs(), config, statLookup, docIdToGroup);
            docIdToGroup = multiCache.getGroupLookup();
            rebuildMultiCache = false;
        }
        return multiCache;
    }

    public NativeShard getNativeShard() { return nativeShard; }

    /* !@# This is a temporary hack that needs to go away. Some
        operations, namely regroup, require both a proper multicache
        (and the packed table within it) and a NativeShard. Since the
        native shard class currently owns the packed table reference,
        which is dubious, we have to reconstruct both multicache and
        native shard in tandem.
     */
    public void bindNativeReferences() {
        if (multiCache == null) {
            final MultiCacheConfig config = new MultiCacheConfig();
            final StatLookup[] statLookups = new StatLookup[1];
            statLookups[0] = statLookup;
            config.calcOrdering(statLookups, numStats);
            buildMultiCache(config);
            if (nativeShard != null) {
                nativeShard.close();
                nativeShard = null;
            }
        }

        if (nativeShard == null) {
            try {
                nativeShard = new NativeShard(simpleFlamdexReader, multiCache.getNativeAddress());
            }
            catch (IOException ex) {
                throw new RuntimeException("failed to create nativeShard", ex);
            }
        }
    }

    @Override
    public synchronized void rebuildAndFilterIndexes(@Nonnull final List<String> intFields,
                                                     @Nonnull final List<String> stringFields) {
        throw new UnsupportedOperationException();
    }

    @Override
    public synchronized long[] getGroupStats(int stat) {

        bindNativeReferences();

        long[] result = groupStats.get(stat);
        if (groupStats.isDirty(stat)) {
            multiCache.nativeGetGroupStats(stat, result);
            groupStats.validate(stat);
        }
        return result;
    }

    @Override
    public synchronized GroupStatsIterator getGroupStatsIterator(int stat) {
        return new GroupStatsDummyIterator(this.getGroupStats(stat));
    }

    @Override
    protected GroupLookup resizeGroupLookup(GroupLookup lookup, final int size,
                                            final MemoryReservationContext memory)
        throws ImhotepOutOfMemoryException {
        return multiCache != null ? multiCache.getGroupLookup() : lookup;
    }

    @Override
    protected void freeDocIdToGroup() { }

    @Override
    protected void tryClose() {
        if (multiCache != null) {
            multiCache.close();
            // TODO: free memory?
        }
        if (nativeShard != null) {
            nativeShard.close();
        }
        super.tryClose();
    }

    @Override
    public synchronized int regroup(final GroupMultiRemapRule[] rules, boolean errorOnCollisions)
        throws ImhotepOutOfMemoryException {

        int result = 0;
        bindNativeReferences();

        final long rulesPtr = nativeGetRules(rules);
        try {
            result = nativeRegroup(rulesPtr, nativeShard.getPtr(), errorOnCollisions);
            docIdToGroup.recalculateNumGroups();
            groupStats.reset(numStats, result);
        }
        finally {
            nativeReleaseRules(rulesPtr);
        }
        return result;
    }

    private native static long nativeGetRules(final GroupMultiRemapRule[] rules);
    private native static void nativeReleaseRules(final long nativeRulesPtr);
    private native static int  nativeRegroup(final long nativeRulesPtr,
                                             final long nativeShardDataPtr,
                                             final boolean errorOnCollisions);
}

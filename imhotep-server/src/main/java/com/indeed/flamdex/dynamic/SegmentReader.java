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

package com.indeed.flamdex.dynamic;

import com.google.common.base.Optional;
import com.google.common.base.Supplier;
import com.indeed.flamdex.api.DocIdStream;
import com.indeed.flamdex.api.FieldsCardinalityMetadata;
import com.indeed.flamdex.api.FlamdexOutOfMemoryException;
import com.indeed.flamdex.api.FlamdexReader;
import com.indeed.flamdex.api.IntTermDocIterator;
import com.indeed.flamdex.api.IntTermIterator;
import com.indeed.flamdex.api.IntValueLookup;
import com.indeed.flamdex.api.StringTermDocIterator;
import com.indeed.flamdex.api.StringTermIterator;
import com.indeed.flamdex.api.StringValueLookup;
import com.indeed.flamdex.api.TermIterator;
import com.indeed.flamdex.datastruct.FastBitSet;
import com.indeed.flamdex.query.Query;
import com.indeed.flamdex.reader.GenericFlamdexReader;
import com.indeed.flamdex.reader.GenericStringToIntTermIterator;
import com.indeed.flamdex.search.FlamdexSearcher;
import com.indeed.util.core.io.Closeables2;
import it.unimi.dsi.fastutil.ints.AbstractIntIterator;
import it.unimi.dsi.fastutil.ints.IntIterator;
import it.unimi.dsi.fastutil.ints.IntIterators;
import org.apache.log4j.Logger;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collection;

/**
 * Simple implementation of {@link FlamdexReader} with tombstoneSet (bitset for deleted docIds).
 *
 * @author michihiko
 */

public class SegmentReader implements FlamdexReader {
    private static final Logger LOG = Logger.getLogger(SegmentReader.class);

    private final FlamdexReader flamdexReader;
    private final FastBitSet tombstoneSet;

    SegmentReader(@Nonnull final Path segmentDirectory) throws IOException {
        this.flamdexReader = GenericFlamdexReader.open(segmentDirectory);
        this.tombstoneSet = DynamicFlamdexSegmentUtil.readTombstoneSet(segmentDirectory).orNull();
    }

    boolean isDeleted(final int docId) {
        return (tombstoneSet != null) && tombstoneSet.get(docId);
    }

    @Nonnull
    Optional<FastBitSet> getUpdatedTombstoneSet(@Nonnull final Query query) {
        final FlamdexSearcher flamdexSearcher = new FlamdexSearcher(flamdexReader);
        final FastBitSet newTombstoneSet = flamdexSearcher.search(query);
        if (tombstoneSet == null) {
            if (newTombstoneSet.isEmpty()) {
                return Optional.absent();
            }
            return Optional.of(newTombstoneSet);
        }
        final int pre = tombstoneSet.cardinality();
        newTombstoneSet.or(tombstoneSet);
        final int post = newTombstoneSet.cardinality();
        if (pre == post) {
            return Optional.absent();
        } else {
            return Optional.of(newTombstoneSet);
        }
    }

    @Override
    public IntIterator getDeletedDocIterator() {
        if (tombstoneSet == null) {
            return IntIterators.EMPTY_ITERATOR;
        } else {
            return new DeletedDocIterator();
        }
    }

    private final class DeletedDocIterator extends AbstractIntIterator {
        final FastBitSet.IntIterator inner = tombstoneSet.iterator();
        boolean hasNext = inner.next();

        @Override
        public boolean hasNext() {
            return hasNext;
        }

        @Override
        public int nextInt() {
            final int value = inner.getValue();
            hasNext = inner.next();
            return value;
        }
    }

    @Override
    public void close() throws IOException {
        Closeables2.closeAll(LOG, flamdexReader);
    }

    @Override
    public Collection<String> getIntFields() {
        return flamdexReader.getIntFields();
    }

    @Override
    public Collection<String> getStringFields() {
        return flamdexReader.getStringFields();
    }

    int maxNumDocs() {
        return flamdexReader.getNumDocs();
    }

    @Override
    public int getNumDocs() {
        throw new UnsupportedOperationException("We don't need to implement this method");
    }

    @Override
    public Path getDirectory() {
        return flamdexReader.getDirectory();
    }

    @Override
    public DocIdStream getDocIdStream() {
        return new DocIdStream() {
            final DocIdStream docIdStream = flamdexReader.getDocIdStream();
            int bufferPos = 0;
            int bufferLength = 0;
            int[] internalBuffer = null;

            @Override
            public void reset(final TermIterator term) {
                if (term instanceof GenericStringToIntTermIterator) {
                    docIdStream.reset(((GenericStringToIntTermIterator) term).getCurrentStringTermIterator());
                } else {
                    docIdStream.reset(term);
                }
            }

            @Override
            public int fillDocIdBuffer(final int[] docIdBuffer) {
                if (internalBuffer == null) {
                    internalBuffer = new int[docIdBuffer.length];
                } else if (internalBuffer.length < (docIdBuffer.length / 2)) {
                    internalBuffer = Arrays.copyOf(internalBuffer, docIdBuffer.length);
                }
                int numFilled = 0;
                while (numFilled < docIdBuffer.length) {
                    if (bufferPos == bufferLength) {
                        bufferPos = 0;
                        bufferLength = docIdStream.fillDocIdBuffer(internalBuffer);
                        if (bufferLength == 0) {
                            break;
                        }
                    }
                    final int docId = internalBuffer[bufferPos];
                    bufferPos++;
                    if ((tombstoneSet == null) || !tombstoneSet.get(docId)) {
                        docIdBuffer[numFilled] = docId;
                        numFilled++;
                    }
                }
                return numFilled;
            }

            @Override
            public void close() {
                docIdStream.close();
            }
        };
    }

    @Override
    public IntTermIterator getUnsortedIntTermIterator(final String field) {
        throw new UnsupportedOperationException("We don't need to implement this method");
    }

    @Override
    public IntTermIterator getIntTermIterator(final String field) {
        return flamdexReader.getIntTermIterator(field);
    }

    @Override
    public StringTermIterator getStringTermIterator(final String field) {
        return flamdexReader.getStringTermIterator(field);
    }

    IntTermIterator getStringToIntTermIterator(final String field) {
        return new GenericStringToIntTermIterator<>(
                getStringTermIterator(field),
                new Supplier<StringTermIterator>() {
                    @Override
                    public StringTermIterator get() {
                        return getStringTermIterator(field);
                    }
                }
        );
    }

    @Override
    public IntTermDocIterator getIntTermDocIterator(final String field) {
        throw new UnsupportedOperationException("We don't need to implement this method");
    }

    @Override
    public StringTermDocIterator getStringTermDocIterator(final String field) {
        throw new UnsupportedOperationException("We don't need to implement this method");
    }

    @Override
    public long memoryRequired(final String metric) {
        return flamdexReader.memoryRequired(metric);
    }

    @Override
    public FieldsCardinalityMetadata getFieldsMetadata() {
        return null;
    }

    @Override
    public long getIntTotalDocFreq(final String field) {
        return flamdexReader.getIntTotalDocFreq(field);
    }

    @Override
    public long getStringTotalDocFreq(final String field) {
        return flamdexReader.getStringTotalDocFreq(field);
    }

    @Override
    public Collection<String> getAvailableMetrics() {
        return flamdexReader.getAvailableMetrics();
    }

    @Override
    public IntValueLookup getMetric(final String metric) throws FlamdexOutOfMemoryException {
        return flamdexReader.getMetric(metric);
    }

    @Override
    public StringValueLookup getStringLookup(final String field) throws FlamdexOutOfMemoryException {
        return flamdexReader.getStringLookup(field);
    }
}

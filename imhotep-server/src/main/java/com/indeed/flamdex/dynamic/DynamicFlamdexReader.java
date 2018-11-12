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

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Ordering;
import com.google.common.io.Closer;
import com.indeed.flamdex.api.DocIdStream;
import com.indeed.flamdex.api.FieldsCardinalityMetadata;
import com.indeed.flamdex.api.FlamdexOutOfMemoryException;
import com.indeed.flamdex.api.FlamdexReader;
import com.indeed.flamdex.api.GenericIntTermDocIterator;
import com.indeed.flamdex.api.GenericStringTermDocIterator;
import com.indeed.flamdex.api.IntTermDocIterator;
import com.indeed.flamdex.api.IntTermIterator;
import com.indeed.flamdex.api.IntValueLookup;
import com.indeed.flamdex.api.StringTermDocIterator;
import com.indeed.flamdex.api.StringTermIterator;
import com.indeed.flamdex.api.StringValueLookup;
import com.indeed.util.core.io.Closeables2;
import it.unimi.dsi.fastutil.ints.AbstractIntIterator;
import it.unimi.dsi.fastutil.ints.IntIterator;
import it.unimi.dsi.fastutil.ints.IntIterators;
import org.apache.log4j.Logger;

import javax.annotation.Nonnull;
import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

/**
 * FlamdexReader for dynamic index.
 * The return values of getNumDocs, docFreq(), ... might be wrong because of lazied deletion.
 *
 * @author michihiko
 */

public class DynamicFlamdexReader implements FlamdexReader {
    private static final Logger LOG = Logger.getLogger(DynamicFlamdexReader.class);
    private static final IntTermIterator EMPTY_INTTERM_ITERATOR = new IntTermIterator() {
        @Override
        public void reset(final long term) {
        }

        @Override
        public long term() {
            return 0;
        }

        @Override
        public boolean next() {
            return false;
        }

        @Override
        public int docFreq() {
            return 0;
        }

        @Override
        public void close() {
        }
    };

    private final Path indexDirectory;
    private final Closeable lock;
    private final List<SegmentReader> segmentReaders;
    private final int[] offsets;

    public DynamicFlamdexReader(final Path directory) throws IOException {
        final Closer closerOnFailure = Closer.create();
        try {
            this.lock = closerOnFailure.register(DynamicFlamdexIndexUtil.acquireReaderLock(directory));

            this.indexDirectory = directory;
            final ImmutableList.Builder<SegmentReader> segmentReadersBuilder = ImmutableList.builder();
            for (final Path segmentDirectory : DynamicFlamdexIndexUtil.listSegmentDirectories(directory)) {
                segmentReadersBuilder.add(closerOnFailure.register(new SegmentReader(segmentDirectory)));
            }
            this.segmentReaders = segmentReadersBuilder.build();
        } catch (final Throwable e) {
            Closeables2.closeQuietly(closerOnFailure, LOG);
            throw e;
        }

        this.offsets = new int[this.segmentReaders.size() + 1];
        for (int i = 0; i < this.segmentReaders.size(); ++i) {
            this.offsets[i + 1] = this.offsets[i] + this.segmentReaders.get(i).maxNumDocs();
        }
    }

    DynamicFlamdexReader(final Path directory, @Nonnull final List<SegmentReader> segmentReaders) {
        this.indexDirectory = directory;
        this.lock = null;
        this.segmentReaders = segmentReaders;
        this.offsets = new int[this.segmentReaders.size() + 1];
        for (int i = 0; i < this.segmentReaders.size(); ++i) {
            this.offsets[i + 1] = this.offsets[i] + this.segmentReaders.get(i).maxNumDocs();
        }
    }

    int getNumberOfSegments() {
        return segmentReaders.size();
    }

    @Override
    public Collection<String> getIntFields() {
        return FluentIterable.from(segmentReaders).transformAndConcat(new Function<SegmentReader, Collection<String>>() {
            @Override
            public Collection<String> apply(final SegmentReader segmentReader) {
                return segmentReader.getIntFields();
            }
        }).toSortedSet(Ordering.natural());
    }

    @Override
    public Collection<String> getStringFields() {
        return FluentIterable.from(segmentReaders).transformAndConcat(new Function<SegmentReader, Collection<String>>() {
            @Override
            public Collection<String> apply(final SegmentReader segmentReader) {
                return segmentReader.getStringFields();
            }
        }).toSortedSet(Ordering.natural());
    }

    @Override
    public int getNumDocs() {
        // We can calc actual number using tombstone bitset, but it seems we have to return (max possible doc id) + 1
        return offsets[segmentReaders.size()];
    }

    @Override
    public Path getDirectory() {
        return indexDirectory;
    }

    @Override
    public DocIdStream getDocIdStream() {
        return new MergedDocIdStream(
                FluentIterable.from(segmentReaders).transform(new Function<SegmentReader, DocIdStream>() {
                    @Override
                    public DocIdStream apply(final SegmentReader segmentReader) {
                        return segmentReader.getDocIdStream();
                    }
                }).toList(),
                offsets
        );
    }

    @Override
    public IntTermIterator getUnsortedIntTermIterator(final String field) {
        return getIntTermIterator(field);
    }

    @Override
    public IntTermIterator getIntTermIterator(final String field) {
        if (this.getIntFields().contains(field)) {
            return new MergedIntTermIterator(
                    FluentIterable.from(segmentReaders).transform(new Function<SegmentReader, IntTermIterator>() {
                        @Override
                        public IntTermIterator apply(final SegmentReader segmentReader) {
                            if (segmentReader.getIntFields().contains(field)) {
                                return segmentReader.getIntTermIterator(field);
                            } else {
                                return EMPTY_INTTERM_ITERATOR;
                            }
                        }
                    }).toList()
            );
        } else {
            return new MergedIntTermIterator(
                    FluentIterable.from(segmentReaders).transform(new Function<SegmentReader, IntTermIterator>() {
                        @Override
                        public IntTermIterator apply(final SegmentReader segmentReader) {
                            return segmentReader.getStringToIntTermIterator(field);
                        }
                    }).toList()
            );
        }
    }

    @Override
    public StringTermIterator getStringTermIterator(final String field) {
        return new MergedStringTermIterator(
                FluentIterable.from(segmentReaders).transform(new Function<SegmentReader, StringTermIterator>() {
                    @Override
                    public StringTermIterator apply(final SegmentReader segmentReader) {
                        return segmentReader.getStringTermIterator(field);
                    }
                }).toList()
        );
    }

    @Override
    public IntTermDocIterator getIntTermDocIterator(final String field) {
        return new GenericIntTermDocIterator(getIntTermIterator(field), getDocIdStream());
    }

    @Override
    public StringTermDocIterator getStringTermDocIterator(final String field) {
        return new GenericStringTermDocIterator(getStringTermIterator(field), getDocIdStream());
    }

    @Override
    public long getIntTotalDocFreq(final String field) {
        long totalDocFreq = 0;
        for (final SegmentReader segmentReader : segmentReaders) {
            totalDocFreq += segmentReader.getIntTotalDocFreq(field);
        }
        return totalDocFreq;
    }

    @Override
    public long getStringTotalDocFreq(final String field) {
        long totalDocFreq = 0;
        for (final SegmentReader segmentReader : segmentReaders) {
            totalDocFreq += segmentReader.getStringTotalDocFreq(field);
        }
        return totalDocFreq;
    }

    @Override
    public Collection<String> getAvailableMetrics() {
        return FluentIterable.from(segmentReaders).transformAndConcat(new Function<SegmentReader, Collection<String>>() {
            @Override
            public Collection<String> apply(final SegmentReader segmentReader) {
                return segmentReader.getAvailableMetrics();
            }
        }).toSortedSet(Ordering.<String>natural());
    }

    private int calcSegmentFromDocId(final int docId) {
        Preconditions.checkPositionIndex(docId, offsets[segmentReaders.size()]);
        // calc segmentId = min{ i | offsets[i] <= docId }
        int segmentId = Arrays.binarySearch(offsets, docId);
        if (segmentId < 0) {
            segmentId = (~segmentId) - 1;
        }
        Preconditions.checkPositionIndex(segmentId, segmentReaders.size());
        return segmentId;
    }

    private class MergedIntValueLookup implements IntValueLookup {

        final List<IntValueLookup> intValueLookups;

        private MergedIntValueLookup(@Nonnull final List<IntValueLookup> intValueLookups) {
            this.intValueLookups = intValueLookups;
        }

        @Override
        public long getMin() {
            long min = Long.MAX_VALUE;
            for (final IntValueLookup intValueLookup : intValueLookups) {
                min = Math.min(min, intValueLookup.getMin());
            }
            return min;
        }

        @Override
        public long getMax() {
            long max = Long.MIN_VALUE;
            for (final IntValueLookup intValueLookup : intValueLookups) {
                max = Math.max(max, intValueLookup.getMax());
            }
            return max;
        }

        @Override
        public void lookup(final int[] docIds, final long[] values, int n) {
            n = Math.min(n, docIds.length);
            final int[] sizes = new int[intValueLookups.size()];
            final int[] segmentIds = new int[n];
            for (int i = 0; i < n; ++i) {
                segmentIds[i] = calcSegmentFromDocId(docIds[i]);
                ++sizes[segmentIds[i]];
            }
            final int[][] segmentDocIds = new int[intValueLookups.size()][];
            final long[][] results = new long[intValueLookups.size()][];
            for (int segmentId = 0; segmentId < intValueLookups.size(); ++segmentId) {
                segmentDocIds[segmentId] = new int[sizes[segmentId]];
                results[segmentId] = new long[sizes[segmentId]];
            }
            final int[] pos = new int[intValueLookups.size()];
            for (int i = 0; i < n; ++i) {
                final int segmentId = segmentIds[i];
                segmentDocIds[segmentId][pos[segmentId]] = docIds[i] - offsets[segmentId];
                pos[segmentId]++;
            }
            for (int segmentId = 0; segmentId < intValueLookups.size(); ++segmentId) {
                intValueLookups.get(segmentId).lookup(segmentDocIds[segmentId], results[segmentId], sizes[segmentId]);
            }
            Arrays.fill(pos, 0);
            for (int i = 0; i < n; ++i) {
                final int segmentId = segmentIds[i];
                values[i] = results[segmentId][pos[segmentId]];
                pos[segmentId]++;
            }
        }

        @Override
        public long memoryUsed() {
            long sum = 0;
            for (final IntValueLookup intValueLookup : intValueLookups) {
                sum += intValueLookup.memoryUsed();
            }
            return sum;
        }

        @Override
        public void close() {
            for (final IntValueLookup intValueLookup : intValueLookups) {
                intValueLookup.close();
            }
        }
    }

    private class MergedStringValueLookup implements StringValueLookup {
        final List<StringValueLookup> stringValueLookups;

        private MergedStringValueLookup(@Nonnull final List<StringValueLookup> stringValueLookups) {
            this.stringValueLookups = stringValueLookups;
        }

        @Override
        public long memoryUsed() {
            long sum = 0L;
            for (final StringValueLookup stringValueLookup : stringValueLookups) {
                sum += stringValueLookup.memoryUsed();
            }
            return sum;
        }

        @Override
        public void close() {
            for (final StringValueLookup stringValueLookup : stringValueLookups) {
                stringValueLookup.close();
            }
        }

        @Override
        public String getString(final int docId) {
            final int idx = calcSegmentFromDocId(docId);
            final int segmentwiseDocId = docId - offsets[idx];
            return stringValueLookups.get(idx).getString(segmentwiseDocId);
        }

    }

    @Override
    public IntValueLookup getMetric(final String metric) throws FlamdexOutOfMemoryException {
        final ImmutableList.Builder<IntValueLookup> builder = ImmutableList.builder();
        try {
            for (final SegmentReader segmentReader : segmentReaders) {
                builder.add(segmentReader.getMetric(metric));
            }
        } catch (final FlamdexOutOfMemoryException | RuntimeException e) {
            for (final IntValueLookup intValueLookup : builder.build()) {
                intValueLookup.close();
            }
            throw e;
        }
        return new MergedIntValueLookup(builder.build());
    }

    @Override
    public StringValueLookup getStringLookup(final String field) throws FlamdexOutOfMemoryException {
        final ImmutableList.Builder<StringValueLookup> builder = ImmutableList.builder();
        try {
            for (final SegmentReader segmentReader : segmentReaders) {
                builder.add(segmentReader.getStringLookup(field));
            }
        } catch (final FlamdexOutOfMemoryException | RuntimeException e) {
            for (final StringValueLookup stringValueLookup : builder.build()) {
                stringValueLookup.close();
            }
            throw e;
        }
        return new MergedStringValueLookup(builder.build());
    }

    @Override
    public long memoryRequired(final String metric) {
        long memoryRequired = 0;
        for (final SegmentReader segmentReader : segmentReaders) {
            memoryRequired += segmentReader.memoryRequired(metric);
        }
        return memoryRequired;
    }

    @Override
    public FieldsCardinalityMetadata getFieldsMetadata() {
        return null;
    }

    @Override
    public IntIterator getDeletedDocIterator() {
        return new DeletedDocIterator();
    }

    private final class DeletedDocIterator extends AbstractIntIterator {
        private int currentSegment = -1;
        private IntIterator currentIterator = IntIterators.EMPTY_ITERATOR;

        @Override
        public boolean hasNext() {
            while (!currentIterator.hasNext()) {
                ++currentSegment;
                if (currentSegment >= segmentReaders.size()) {
                    return false;
                }
                currentIterator = segmentReaders.get(currentSegment).getDeletedDocIterator();
            }
            return true;
        }

        @Override
        public int nextInt() {
            if (currentSegment >= segmentReaders.size()) {
                throw new IllegalStateException("No more deleted documents");
            }
            return offsets[currentSegment] + currentIterator.nextInt();
        }
    }

    @Override
    public void close() throws IOException {
        Closeables2.closeAll(LOG, Closeables2.forIterable(LOG, segmentReaders), lock);
    }
}

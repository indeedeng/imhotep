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

package com.indeed.imhotep;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.io.ByteStreams;
import com.google.common.primitives.Doubles;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import com.indeed.imhotep.FTGSBinaryFormat.FieldStat;
import com.indeed.imhotep.StreamUtil.InputStreamWithPosition;
import com.indeed.imhotep.StreamUtil.OutputStreamWithPosition;
import com.indeed.imhotep.api.FTGAIterator;
import com.indeed.imhotep.api.FTGIterator;
import com.indeed.imhotep.api.FTGSIterator;
import com.indeed.imhotep.api.GroupStatsIterator;
import com.indeed.imhotep.api.ImhotepSession;
import com.indeed.imhotep.protobuf.StatsSortOrder;
import com.indeed.imhotep.scheduling.SilentCloseable;
import com.indeed.imhotep.service.FTGSOutputStreamWriter;
import com.indeed.imhotep.utils.BoundedPriorityQueue;
import com.indeed.util.core.Pair;
import com.indeed.util.core.Throwables2;
import com.indeed.util.core.io.Closeables2;
import gnu.trove.map.hash.TIntObjectHashMap;
import gnu.trove.procedure.TObjectProcedure;
import org.apache.commons.lang.mutable.MutableInt;
import org.apache.log4j.Logger;

import javax.annotation.WillClose;
import javax.annotation.WillCloseWhenClosed;
import javax.annotation.WillNotClose;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;

/**
 * @author kenh
 */

public class FTGSIteratorUtil {

    private FTGSIteratorUtil() {
    }

    public static Pair<File, FieldStat[]> persistAsFile(
            final Logger log,
            final String sessionId,
            @WillClose final FTGSIterator iterator
    ) throws IOException {
        try {
            final File tmp = File.createTempFile("ftgs", ".tmp");
            final FieldStat[] stats;
            final long start = System.currentTimeMillis();
            try (final OutputStream out = new BufferedOutputStream(new FileOutputStream(tmp))) {
                stats = writeFtgsIteratorToStream(iterator, out);
                if (log.isDebugEnabled()) {
                    log.debug("[" + sessionId + "] time to merge splits to file: " +
                            (System.currentTimeMillis() - start) +
                            " ms, file length: " + tmp.length());
                }
            } catch (final Throwable t) {
                tmp.delete();
                throw Throwables2.propagate(t, IOException.class);
            }
            return Pair.of(tmp, stats);
        } finally {
            Closeables2.closeQuietly(iterator, log);
        }
    }

    // TODO: A bit too much code duplication here.
    public static Pair<File, FieldStat[]> persistAsFile(
            final Logger log,
            final String sessionId,
            @WillClose final FTGAIterator iterator
    ) throws IOException {
        try {
            final File tmp = File.createTempFile("ftgs", ".tmp");
            final FieldStat[] stats;
            final long start = System.currentTimeMillis();
            try (final OutputStream out = new BufferedOutputStream(new FileOutputStream(tmp))) {
                stats = writeFtgaIteratorToStream(iterator, out);
                if (log.isDebugEnabled()) {
                    log.debug("[" + sessionId + "] time to merge splits to file: " +
                            (System.currentTimeMillis() - start) +
                            " ms, file length: " + tmp.length());
                }
            } catch (final Throwable t) {
                tmp.delete();
                throw Throwables2.propagate(t, IOException.class);
            }
            return Pair.of(tmp, stats);
        } finally {
            Closeables2.closeQuietly(iterator, log);
        }
    }

    public static FTGSIterator persist(final Logger log, @WillClose final FTGSIterator iterator) throws IOException {
        return persist(log, "noSessionId", iterator);
    }

    public static FTGSIterator persist(
            final Logger log,
            final String sessionId,
            @WillClose final FTGSIterator iterator
    ) throws IOException {
        final int numStats = iterator.getNumStats();
        final int numGroups = iterator.getNumGroups();
        final Pair<File, FieldStat[]> tmp = persistAsFile(log, sessionId, iterator);
        try {
            return InputStreamFTGSIterators.create(tmp, numStats, numGroups);
        } finally {
            tmp.getFirst().delete();
        }
    }

    public static FTGAIterator persist(
            final Logger log,
            final String sessionId,
            @WillClose final FTGAIterator iterator
    ) throws IOException {
        final int numStats = iterator.getNumStats();
        final int numGroups = iterator.getNumGroups();
        final Pair<File, FieldStat[]> tmp = persistAsFile(log, sessionId, iterator);
        final File file = tmp.getFirst();
        try {
            return new InputStreamFTGAIterator(new BufferedInputStream(new FileInputStream(file)), tmp.getSecond(), numStats, numGroups);
        } finally {
            file.delete();
        }
    }

    public static TopTermsFTGSIterator getTopTermsFTGSIterator(
            @WillClose final FTGSIterator originalIterator,
            final long termLimit,
            final int sortStat,
            final StatsSortOrder statsSortOrder) {
        try (final SilentCloseable ignored = originalIterator) {
            Preconditions.checkArgument(termLimit > 0, "TopTerms expects positive termLimit");
            Preconditions.checkArgument(sortStat >= 0, "TopTerms expects sortStat index >= 0");
            Preconditions.checkArgument(statsSortOrder != StatsSortOrder.UNDEFINED, "TopTerms does not accept UNDEFINED stats sort order");
            return getTopTermsFTGSIteratorInternal(originalIterator, termLimit, sortStat, statsSortOrder);
        }
    }

    public static TopTermsFTGAIterator getTopTermsFTGSIterator(
            @WillClose final FTGAIterator originalIterator,
            final long termLimit,
            final int sortStat,
            final StatsSortOrder statsSortOrder) {
        try (final SilentCloseable ignored = originalIterator) {
            Preconditions.checkArgument(termLimit > 0, "TopTerms expects positive termLimit");
            Preconditions.checkArgument(sortStat >= 0, "TopTerms expects sortStat index >= 0");
            Preconditions.checkArgument(statsSortOrder != StatsSortOrder.UNDEFINED, "TopTerms does not accept UNDEFINED stats sort order");
            return getTopTermsFTGSIteratorInternal(originalIterator, termLimit, sortStat, statsSortOrder);
        }
    }

    // Consume iterator, sort by terms and return sorted.
    // For testing purposes only!
    // Use this only in tests with small iterators
    @VisibleForTesting
    public static FTGSIterator sortFTGSIterator(@WillClose final FTGSIterator originalIterator) {
        return getTopTermsFTGSIteratorInternal(originalIterator, Integer.MAX_VALUE, -1, StatsSortOrder.ASCENDING);
    }

    // Consume iterator, sort by terms and return sorted.
    // For testing purposes only!
    // Use this only in tests with small iterators
    @VisibleForTesting
    public static FTGAIterator sortFTGSIterator(@WillClose final FTGAIterator originalIterator) {
        return getTopTermsFTGSIteratorInternal(originalIterator, Integer.MAX_VALUE, -1, StatsSortOrder.ASCENDING);
    }

    // Returns top terms iterator.
    // It's possible to pass termLimit = Long.MAX_VALUE and get sorted iterator as a result
    private static TopTermsFTGSIterator getTopTermsFTGSIteratorInternal(
            @WillClose final FTGSIterator originalIterator,
            final long termLimit,
            final int sortStat,
            final StatsSortOrder statsSortOrder) {
        final int numStats = originalIterator.getNumStats();
        final int numGroups = originalIterator.getNumGroups();
        // We don't care about sorted stuff since we will sort by term afterward
        try (final FTGSIterator iterator = makeUnsortedIfPossible(originalIterator)) {
            final TopTermsStatsByField<long[]> topTerms = extractTopTermsGeneric(termLimit, iterator, new LongStatExtractor(iterator.getNumStats(), sortStat, statsSortOrder));
            return new TopTermsFTGSIterator(topTerms, numStats, numGroups);
        }
    }

    // Returns top terms iterator.
    // It's possible to pass termLimit = Long.MAX_VALUE and get sorted iterator as a result
    private static TopTermsFTGAIterator getTopTermsFTGSIteratorInternal(
            @WillClose final FTGAIterator iterator,
            final long termLimit,
            final int sortStat,
            final StatsSortOrder statsSortOrder
    ) {
        final int numStats = iterator.getNumStats();
        final int numGroups = iterator.getNumGroups();
        try {
            final TopTermsStatsByField<double[]> topTerms = FTGSIteratorUtil.extractTopTermsGeneric(termLimit, iterator, new DoubleStatExtractor(iterator.getNumStats(), sortStat, statsSortOrder));
            return new TopTermsFTGAIterator(topTerms, numStats, numGroups);
        } finally {
            iterator.close();
        }
    }

    /**
     * Used for extracting the top-K sort stat from an iterator in a generic way
     *
     * advance will be called with the iterator prior to a call to itIsBetterThan or extract,
     *   but the iterator is still provided with those methods for convenience and avoiding
     *   redundant work when the term is not going to be kept anyway
     *
     * It is expected that itIsBetterThan(it, termStat) <=> (extract(it).compareTo(termStat) > 0)
     *
     * @param <S> The type of generic stats contained in the TermStat for purposes other than top-K
     * @param <IT> The iterator type being extracted from
     */
    @VisibleForTesting
    interface StatExtractor<S, IT> {
        void advance(IT it);
        boolean itIsBetterThan(IT it, TermStat<S> termStat);
        TermStat<S> extract(IT it);
        // Must be consistent with itIsBetterThan as described in docs above
        Comparator<TermStat<S>> comparator();
    }

    @VisibleForTesting
    static class LongStatExtractor implements StatExtractor<long[], FTGSIterator> {
        private final int sortStat;
        private final long[] statsBuf;
        private final StatsSortOrder statsSortOrder;

        @VisibleForTesting
        LongStatExtractor(int numStats, int sortStat, StatsSortOrder statsSortOrder) {
            this.statsBuf = new long[numStats];
            this.sortStat = sortStat;
            this.statsSortOrder = statsSortOrder;
        }

        @Override
        public void advance(FTGSIterator iterator) {
            iterator.groupStats(statsBuf);
        }

        @Override
        public boolean itIsBetterThan(FTGSIterator iterator, TermStat<long[]> termStat) {
            final int statCmp = Long.compare(statsBuf[sortStat], termStat.groupStats[sortStat]);
            if (statCmp != 0) {
                if (statsSortOrder == StatsSortOrder.DESCENDING) {
                    return statCmp < 0;
                }
                else {
                    return statCmp > 0;
                }
            }

            boolean r;
            if (iterator.fieldIsIntType()) {
                r = iterator.termIntVal() < termStat.intTerm;
            } else {
                r = AbstractBatchedFTGMerger.compareBytes(
                        iterator.termStringBytes(),
                        iterator.termStringLength(),
                        termStat.strTermBytes,
                        termStat.strTermBytes.length) < 0;
            }
            if (statsSortOrder == StatsSortOrder.DESCENDING) {
                return !r;
            }
            return r;
        }

        @Override
        public TermStat<long[]> extract(@WillNotClose FTGSIterator iterator) {
            final boolean fieldIsIntType = iterator.fieldIsIntType();
            final long termIntVal = fieldIsIntType ? iterator.termIntVal() : 0;
            final byte[] termStringBytes = fieldIsIntType ? null : Arrays.copyOf(iterator.termStringBytes(), iterator.termStringLength());
            final long termDocFreq = iterator.termDocFreq();
            return new TermStat<>(fieldIsIntType, termIntVal, termStringBytes, termDocFreq, iterator.group(), statsBuf.clone());
        }

        @Override
        public Comparator<TermStat<long[]>> comparator() {
            Comparator<TermStat<long[]>> termStatComparator = new Comparator<TermStat<long[]>>() {
                @Override
                public int compare(final TermStat<long[]> x, final TermStat<long[]> y) {
                    // Support for `sortStat < 0` is solely for unit test usage.
                    // It will only be invoked when max # terms == Long.MAX_VALUE, so does
                    // not need to be supported in itIsBetterThan.
                    // Feel free to remove it and fix broken unit tests if it measurably better.
                    final int ret = sortStat < 0 ? 0 : Longs.compare(x.groupStats[sortStat], y.groupStats[sortStat]);
                    if (ret == 0) {
                        if (x.fieldIsIntType) {
                            return Longs.compare(y.intTerm, x.intTerm);
                        } else {
                            return stringTermBytesCompareTo(y.strTermBytes, x.strTermBytes);
                        }
                    }
                    return ret;
                }
            };
            if (statsSortOrder == StatsSortOrder.DESCENDING) {
                return termStatComparator.reversed();
            } else {
                return termStatComparator;
            }
        }
    }

    @VisibleForTesting
    static class DoubleStatExtractor implements StatExtractor<double[], FTGAIterator> {
        private final int sortStat;
        private final double[] statsBuf;
        private final StatsSortOrder statsSortOrder;

        @VisibleForTesting
        DoubleStatExtractor(final int numStats, final int sortStat, final StatsSortOrder statsSortOrder) {
            this.statsBuf = new double[numStats];
            this.sortStat = sortStat;
            this.statsSortOrder = statsSortOrder;
        }

        @Override
        public void advance(@WillNotClose final FTGAIterator iterator) {
            iterator.groupStats(statsBuf);
        }

        @Override
        public boolean itIsBetterThan(@WillNotClose final FTGAIterator iterator, final TermStat<double[]> termStat) {
            final int statCmp = Double.compare(statsBuf[sortStat], termStat.groupStats[sortStat]);
            if (statCmp != 0) {
                if (statsSortOrder == StatsSortOrder.DESCENDING)
                    return statCmp < 0;
                else
                    return statCmp > 0;
            }

            boolean r;
            if (iterator.fieldIsIntType()) {
                r = iterator.termIntVal() < termStat.intTerm;
            } else {
                r = AbstractBatchedFTGMerger.compareBytes(
                        iterator.termStringBytes(),
                        iterator.termStringLength(),
                        termStat.strTermBytes,
                        termStat.strTermBytes.length) < 0;
            }
            if (statsSortOrder == StatsSortOrder.DESCENDING) {
                return !r;
            }
            return r;
        }

        @Override
        public TermStat<double[]> extract(@WillNotClose final FTGAIterator iterator) {
            final boolean fieldIsIntType = iterator.fieldIsIntType();
            final long termIntVal = fieldIsIntType ? iterator.termIntVal() : 0;
            final byte[] termStringBytes = fieldIsIntType ? null : Arrays.copyOf(iterator.termStringBytes(), iterator.termStringLength());
            final long termDocFreq = iterator.termDocFreq();
            return new TermStat<>(fieldIsIntType, termIntVal, termStringBytes, termDocFreq, iterator.group(), statsBuf.clone());
        }

        @Override
        public Comparator<TermStat<double[]>> comparator() {
            Comparator<TermStat<double[]>> termStatComparator =  new Comparator<TermStat<double[]>>() {
                @Override
                public int compare(final TermStat<double[]> x, final TermStat<double[]> y) {
                    // Support for `sortStat < 0` is solely for unit test usage.
                    // It will only be invoked when max # terms == Long.MAX_VALUE, so does
                    // not need to be supported in itIsBetterThan.
                    // Feel free to remove it and fix broken unit tests if it measurably better.
                    final int ret = (sortStat < 0) ? 0 : Doubles.compare(x.groupStats[sortStat], y.groupStats[sortStat]);
                    if (ret == 0) {
                        if (x.fieldIsIntType) {
                            return Longs.compare(y.intTerm, x.intTerm);
                        } else {
                            return stringTermBytesCompareTo(y.strTermBytes, x.strTermBytes);
                        }
                    }
                    return ret;
                }
            };
            if (statsSortOrder == StatsSortOrder.DESCENDING) {
                return termStatComparator.reversed();
            } else {
                return termStatComparator;
            }
        }
    }

    private static <IT extends FTGIterator, S> TopTermsStatsByField<S> extractTopTermsGeneric(
            final long termLimit,
            @WillNotClose final IT iterator,
            final StatExtractor<S, IT> extractor
    ) {
        if (termLimit > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("termLimit is over limit, it should be a valid integer");
        }

        final TopTermsStatsByField<S> topTermsFTGS = new TopTermsStatsByField<>();
        final Comparator<TermStat<S>> comparator = extractor.comparator();

        while (iterator.nextField()) {
            final String fieldName = iterator.fieldName();
            final boolean fieldIsIntType = iterator.fieldIsIntType();

            final TIntObjectHashMap<BoundedPriorityQueue<TermStat<S>>> topTermsByGroup = new TIntObjectHashMap<>();

            while (iterator.nextTerm()) {
                while (iterator.nextGroup()) {
                    BoundedPriorityQueue<TermStat<S>> topTerms = topTermsByGroup.get(iterator.group());
                    if (topTerms == null) {
                        topTerms = new BoundedPriorityQueue<>((int)termLimit, comparator);
                        topTermsByGroup.put(iterator.group(), topTerms);
                    }

                    extractor.advance(iterator);
                    topTerms.offer(extractor.extract(iterator));
                }
            }

            final MutableInt termsAndGroups = new MutableInt(0);
            topTermsByGroup.forEachValue(topTerms -> {
                termsAndGroups.add(topTerms.size());
                return true;
            });

            final TermStat<S>[] topTermsArray = new TermStat[termsAndGroups.intValue()];

            topTermsByGroup.forEachValue(new TObjectProcedure<BoundedPriorityQueue<TermStat<S>>>() {
                private int i = 0;
                @Override
                public boolean execute(final BoundedPriorityQueue<TermStat<S>> topTerms) {
                    topTerms.getTopK(topTermsArray, i);
                    i += topTerms.size();
                    return true;
                }
            });
            Arrays.sort(topTermsArray, new TermStat.TermGroupComparator());
            topTermsFTGS.addField(fieldName, fieldIsIntType, topTermsArray);
        }

        return topTermsFTGS;
    }

    static class TermStat<S> {
        final boolean fieldIsIntType;
        final long intTerm;
        // valid stringTerm bytes in the range [0, termStringLength)
        final byte[] strTermBytes;
        final long termDocFreq;
        final int group;
        final S groupStats;

        TermStat(final boolean fieldIsIntType, final long intTerm, final byte[] strTermBytes, final long termDocFreq, final int group, final S groupStats) {
            this.fieldIsIntType = fieldIsIntType;
            this.intTerm = intTerm;
            this.strTermBytes = strTermBytes;
            this.termDocFreq = termDocFreq;
            this.group = group;
            this.groupStats = groupStats;
        }

        boolean haveSameTerm(final FTGSIteratorUtil.TermStat other) {
            return (fieldIsIntType == other.fieldIsIntType)
                    && (fieldIsIntType ? (intTerm == other.intTerm) : (stringTermBytesCompareTo(strTermBytes, other.strTermBytes) == 0));
        }

        private static class TermGroupComparator implements Comparator<TermStat> {
            @Override
            public int compare(final TermStat x, final TermStat y) {
                final int ret;
                if (x.fieldIsIntType) {
                    ret = Longs.compare(x.intTerm, y.intTerm);
                } else {
                    ret = stringTermBytesCompareTo(x.strTermBytes, y.strTermBytes);
                }

                if (ret != 0) {
                    return ret;
                }
                return Ints.compare(x.group, y.group);
            }
        }
    }

    static class TopTermsStatsByField<S> {
        static class FieldAndTermStats<S> {
            final String field;
            final boolean isIntType;
            final TermStat<S>[] termStats;

            FieldAndTermStats(final String field, final boolean isIntType, final TermStat<S>[] termStats) {
                this.field = field;
                this.isIntType = isIntType;
                this.termStats = termStats;
            }
        }

        private final List<FieldAndTermStats<S>> fieldTermStatsList = new ArrayList<>();

        void addField(final String field, final boolean isIntType, final TermStat<S>[] terms) {
            fieldTermStatsList.add(new FieldAndTermStats<>(field, isIntType, terms));
        }

        List<FieldAndTermStats<S>> getEntries() {
            return fieldTermStatsList;
        }
    }

    public static FTGSIterator makeUnsortedIfPossible(@WillCloseWhenClosed final FTGSIterator iterator) {
        if (iterator instanceof SortedFTGSInterleaver) {
            final FTGSIterator[] iterators = ((SortedFTGSInterleaver) iterator).getIterators();
            return new UnsortedFTGSIterator(iterators);
        }
        return iterator;
    }

    public static GroupStatsIterator calculateDistinct(@WillClose final FTGSIterator iterator) {
        try (final FTGSIterator unsortedFtgs = FTGSIteratorUtil.makeUnsortedIfPossible(iterator)) {

            if (!unsortedFtgs.nextField()) {
                throw new IllegalArgumentException("FTGSIterator with at least one field expected");
            }

            final long[] result = new long[unsortedFtgs.getNumGroups()];
            while (unsortedFtgs.nextTerm()) {
                while (unsortedFtgs.nextGroup()) {
                    final int group = unsortedFtgs.group();
                    result[group]++;
                }
            }

            if (unsortedFtgs.nextField()) {
                throw new IllegalArgumentException("FTGSIterator with exactly one field expected");
            }

            return new GroupStatsDummyIterator(result);
        }
    }

    /**
     * This method is a workaround to delete getFTGSIteratorSplits method from ImhotepSession,
     * but provide equivalent functionality to Imhotep clients
     */
    public static FTGSIterator[] getFTGSIteratorSplits(final ImhotepSession session,
                                                       final String[] intFields,
                                                       final String[] stringFields,
                                                       final long termLimit) {
        if (session instanceof RemoteImhotepMultiSession) {
            return ((RemoteImhotepMultiSession)session).getFTGSIteratorSplits(intFields, stringFields, termLimit);
        }

        throw new UnsupportedOperationException();
    }

    public static int getNumStats(@WillNotClose final FTGSIterator[] iterators) {
        if (iterators.length == 0) {
            throw new IllegalArgumentException("Nonempty array of iterators expected.");
        }
        final int numStats = iterators[0].getNumStats();
        for (final FTGSIterator iterator : iterators) {
            if (iterator.getNumStats() != numStats) {
                throw new IllegalArgumentException();
            }
        }
        return numStats;
    }

    public static int getNumStats(@WillNotClose final FTGAIterator[] iterators) {
        if (iterators.length == 0) {
            throw new IllegalArgumentException("Nonempty array of iterators expected.");
        }
        final int numStats = iterators[0].getNumStats();
        for (final FTGAIterator iterator : iterators) {
            if (iterator.getNumStats() != numStats) {
                throw new IllegalArgumentException();
            }
        }
        return numStats;
    }

    public static int getNumGroups(@WillNotClose final FTGIterator[] iterators) {
        int numGroups = 0;
        for (final FTGIterator iterator : iterators) {
            numGroups = Math.max(numGroups, iterator.getNumGroups());
        }
        return numGroups;
    }

    public static FieldStat[] writeFtgsIteratorToStream(@WillNotClose final FTGSIterator iterator, final OutputStream stream) throws IOException {
        // try write optimized or fall to default stream writer

        FieldStat[] result;
        result = tryWriteInputStreamIterator(iterator, stream);
        if (result != null) {
            return result;
        }

        result = tryWriteUnsortedInputStreamIterators(iterator, stream);
        if (result != null) {
            return result;
        }

        return FTGSOutputStreamWriter.write(iterator, stream);
    }

    public static FieldStat[] writeFtgaIteratorToStream(@WillNotClose final FTGAIterator iterator, final OutputStream stream) throws IOException {
        // try write optimized or fall to default stream writer

        FieldStat[] result;
        result = tryWriteInputStreamIterator(iterator, stream);
        if (result != null) {
            return result;
        }

        result = tryWriteUnsortedInputStreamIterators(iterator, stream);
        if (result != null) {
            return result;
        }

        return FTGSOutputStreamWriter.write(iterator, stream);
    }

    // check if iterator is InputStreamFTGSIterator
    // if yes, copy data without decoding-encoding part
    private static FieldStat[] tryWriteInputStreamIterator(@WillNotClose final FTGIterator iterator, final OutputStream out) throws IOException {
        if (!(iterator instanceof AbstractInputStreamFTGXIterator)) {
            return null;
        }
        final AbstractInputStreamFTGXIterator inputIterator = (AbstractInputStreamFTGXIterator) iterator;
        final Pair<InputStream, Optional<FieldStat[]>> streamAndStats = inputIterator.getStreamAndStats();
        if (!streamAndStats.getSecond().isPresent()) {
            return null;
        }

        final FieldStat[] stats = streamAndStats.getSecond().get();
        final FieldStat lastFieldStat = stats[stats.length-1];

        final long to = lastFieldStat.endPosition;

        // copying data from the very beginning of the stream (including first field header)
        // till last term of last field.
        final InputStream allFields = ByteStreams.limit(streamAndStats.getFirst(), to);
        ByteStreams.copy(allFields, out);
        // finalize last field and whole ftgs stream.
        FTGSBinaryFormat.writeFieldEnd(lastFieldStat.isIntField, out);
        FTGSBinaryFormat.writeFtgsEndTag(out);
        out.flush();

        return stats;
    }

    // check if iterator is unsorted disjoint merger of InputStreamFTGSIterator
    // if yes, copy data with some hack on first/last term in sub-iterators
    private static FieldStat[] tryWriteUnsortedInputStreamIterators(@WillNotClose final FTGIterator iterator, @WillNotClose final OutputStream originalOut) throws IOException {
        if (!(iterator instanceof UnsortedFTGIterator)) {
            return null;
        }

        // check that all sub-iterators have field stats
        final FTGIterator[] subIterators = ((UnsortedFTGIterator<?>) iterator).getIterators();

        final Pair<InputStreamWithPosition, FieldStat[]>[] streamsAndStats = new Pair[subIterators.length];
        for (int i = 0; i < subIterators.length; i++) {
            if (subIterators[i] instanceof InputStreamFTGSIterator) {
                final Pair<InputStream, Optional<FieldStat[]>> stats = ((InputStreamFTGSIterator) subIterators[i]).getStreamAndStats();
                if (!stats.getSecond().isPresent()) {
                    return null;
                }
                final InputStreamWithPosition streamWithPosition = new StreamUtil.InputStreamWithPosition(stats.getFirst());
                streamsAndStats[i] = Pair.of(streamWithPosition, stats.getSecond().get());
            } else {
                return null;
            }
        }

        // check that all field names and field types match in all stats.
        final FieldStat[] firstStat = streamsAndStats[0].getSecond();
        final int fieldsCount = firstStat.length;

        for (final Pair<?, FieldStat[]> streamAndStats : streamsAndStats) {
            final FieldStat[] stats = streamAndStats.getSecond();
            if (stats.length != fieldsCount) {
                throw new IllegalStateException();
            }

            for (int i = 0; i < fieldsCount; i++) {
                if (!firstStat[i].fieldName.equals(stats[i].fieldName)
                        || (firstStat[i].isIntField != stats[i].isIntField)) {
                    throw new IllegalStateException();
                }
            }
        }

        // merging field by field and creating merged stats.
        final StreamUtil.OutputStreamWithPosition out = new OutputStreamWithPosition(originalOut);

        final int iteratorsCount = streamsAndStats.length;
        final FieldStat[] resultStats = new FieldStat[fieldsCount];
        for (int fieldIndex = 0; fieldIndex < fieldsCount; fieldIndex++) {
            final boolean isIntField = firstStat[fieldIndex].isIntField;
            FTGSBinaryFormat.writeFieldStart(isIntField, firstStat[fieldIndex].fieldName, out);

            // stat for merged field
            final FieldStat fieldStat = new FieldStat(firstStat[fieldIndex].fieldName, isIntField);
            fieldStat.startPosition = out.getPosition();

            boolean isFirstIterator = true;
            long lastIntTerm = 0;
            byte[] lastStringTerm = new byte[0];
            for (int iteratorIndex = 0; iteratorIndex < iteratorsCount; iteratorIndex++ ) {
                final InputStreamWithPosition stream = streamsAndStats[iteratorIndex].getFirst();
                final FieldStat stat = streamsAndStats[iteratorIndex].getSecond()[fieldIndex];
                if (!stat.hasTerms()) {
                    continue;
                }
                stream.seekForward(stat.startPosition);
                if (isFirstIterator) {
                    // just save first term. data will be copied starting from stat.startPosition
                    if (isIntField) {
                        fieldStat.firstIntTerm = stat.firstIntTerm;
                    } else {
                        fieldStat.firstStringTerm = stat.firstStringTerm;
                    }
                } else {
                    // not first, hack first term.
                    if (isIntField) {
                        final long firstTerm = FTGSBinaryFormat.readFirstIntTerm(stream);
                        FTGSBinaryFormat.writeIntTermStart(firstTerm, lastIntTerm, out);
                    } else {
                        final byte[] firstTerm = FTGSBinaryFormat.readFirstStringTerm(stream);
                        FTGSBinaryFormat.writeStringTermStart(firstTerm, firstTerm.length, lastStringTerm, lastStringTerm.length, out);
                    }
                }
                // copying data (from first byte of first term if it's a first iterator in this field
                // of from first data of group-stats if it's not first iterator)
                final InputStream terms = ByteStreams.limit(stream, stat.endPosition - stream.getPosition());
                ByteStreams.copy(terms, out);
                if (stream.getPosition() != stat.endPosition) {
                    throw new IllegalStateException();
                }
                if (isIntField) {
                    lastIntTerm = stat.lastIntTerm;
                } else {
                    lastStringTerm = stat.lastStringTerm;
                }
                isFirstIterator = false;
            }
            // finalizing merged stream and merged stat
            if (isIntField) {
                fieldStat.lastIntTerm = lastIntTerm;
            } else {
                fieldStat.lastStringTerm = lastStringTerm;
            }
            fieldStat.endPosition = out.getPosition();

            FTGSBinaryFormat.writeFieldEnd(isIntField, out);
            resultStats[fieldIndex] = fieldStat;
        }
        // finalize whole ftgs stream.
        FTGSBinaryFormat.writeFtgsEndTag(out);
        out.flush();

        return resultStats;
    }

    private static int stringTermBytesCompareTo(final byte[] lhs, final byte[] rhs) {
        return AbstractBatchedFTGMerger.compareBytes(
                lhs,
                lhs.length,
                rhs,
                rhs.length
        );
    }
}

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

import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import com.indeed.imhotep.api.FTGSIterator;
import com.indeed.imhotep.api.GroupStatsIterator;
import com.indeed.imhotep.api.ImhotepSession;
import com.indeed.imhotep.service.FTGSOutputStreamWriter;
import com.indeed.util.core.Throwables2;
import com.indeed.util.core.io.Closeables2;
import gnu.trove.map.hash.TIntObjectHashMap;
import gnu.trove.procedure.TIntObjectProcedure;
import gnu.trove.procedure.TObjectProcedure;
import org.apache.commons.lang.mutable.MutableInt;
import org.apache.log4j.Logger;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

/**
 * @author kenh
 */

public class FTGSIteratorUtil {

    private FTGSIteratorUtil() {
    }

    public static File persistAsFile(final Logger log,
                                     final String sessionId,
                                     final FTGSIterator iterator,
                                     final int numStats) throws IOException {
        final File tmp = File.createTempFile("ftgs", ".tmp");
        OutputStream out = null;
        try {
            final long start = System.currentTimeMillis();
            out = new BufferedOutputStream(new FileOutputStream(tmp));
            FTGSOutputStreamWriter.write(iterator, numStats, out);
            if (log.isDebugEnabled()) {
                log.debug("[" + sessionId + "] time to merge splits to file: " +
                        (System.currentTimeMillis() - start) +
                        " ms, file length: " + tmp.length());
            }
        } catch (final Throwable t) {
            tmp.delete();
            throw Throwables2.propagate(t, IOException.class);
        } finally {
            Closeables2.closeQuietly(iterator, log);
            if (out != null) {
                out.close();
            }
        }

        return tmp;
    }

    public static FTGSIterator persist(final Logger log, final FTGSIterator iterator, final int numStats) throws IOException {
        return persist(log, "noSessionId", iterator, numStats);
    }

    public static FTGSIterator persist(final Logger log,
                                   final String sessionId,
                                   final FTGSIterator iterator,
                                   final int numStats) throws IOException {
        final File tmp = persistAsFile(log, sessionId, iterator, numStats);
        try {
            return InputStreamFTGSIterators.create(tmp, numStats);
        } finally {
            tmp.delete();
        }
    }

    public static TopTermsFTGSIterator getTopTermsFTGSIterator(final FTGSIterator originalIterator, final long termLimit, final int numStats, final int sortStat) {
        if ((termLimit <= 0) || (sortStat < 0) || (sortStat >= numStats)) {
            throw new IllegalArgumentException("TopTerms expect positive termLimit and valid sortStat index");
        }
        return getTopTermsFTGSIteratorInternal(originalIterator, termLimit, numStats, sortStat);
    }

    // Consume iterator, sort by terms and return sorted.
    // For testing purposes only!
    // Use this only in tests with small iterators
    public static FTGSIterator sortFTGSIterator(final FTGSIterator originalIterator, final int numStats) {
        return getTopTermsFTGSIteratorInternal(originalIterator, Long.MAX_VALUE, numStats, -1);
    }

    // Returns top terms iterator.
    // It's possible to pass termLimit = Long.MAX_VALUE and get sorted iterator as a result
    private static TopTermsFTGSIterator getTopTermsFTGSIteratorInternal(final FTGSIterator originalIterator, final long termLimit, final int numStats, final int sortStat) {
        try {
            final long[] statBuf = new long[numStats];
            final TopTermsStatsByField topTermsFTGS = new TopTermsStatsByField();

            // We don't care about sorted stuff since we will sort by term afterward
            final FTGSIterator iterator = makeUnsortedIfPossible(originalIterator);

            while (iterator.nextField()) {
                final String fieldName = iterator.fieldName();
                final boolean fieldIsIntType = iterator.fieldIsIntType();

                final TIntObjectHashMap<PriorityQueue<TermStat>> topTermsByGroup = new TIntObjectHashMap<>();

                while (iterator.nextTerm()) {
                    final long termIntVal = fieldIsIntType ? iterator.termIntVal() : 0;
                    final String termStringVal = fieldIsIntType ? null : iterator.termStringVal();
                    final long termDocFreq = iterator.termDocFreq();

                    while (iterator.nextGroup()) {
                        PriorityQueue<TermStat> topTerms = topTermsByGroup.get(iterator.group());
                        if (topTerms == null) {
                            topTerms = new PriorityQueue<>(10, TermStat.TOP_STAT_COMPARATOR);
                            topTermsByGroup.put(iterator.group(), topTerms);
                        }

                        iterator.groupStats(statBuf);

                        if (topTerms.size() >= termLimit) {
                            final long stat = statBuf[sortStat];
                            final TermStat termStat = new TermStat(fieldIsIntType, termIntVal, termStringVal, termDocFreq, iterator.group(), stat, statBuf.clone());
                            if (TermStat.TOP_STAT_COMPARATOR.compare(termStat, topTerms.peek()) > 0) {
                                topTerms.poll();
                                topTerms.offer(termStat);
                            }
                        } else {
                            final long stat = (sortStat >= 0) ? statBuf[sortStat] : 0;
                            final TermStat termStat = new TermStat(fieldIsIntType, termIntVal, termStringVal, termDocFreq, iterator.group(), stat, statBuf.clone());
                            topTerms.offer(termStat);
                        }
                    }
                }

                final MutableInt termsAndGroups = new MutableInt(0);
                topTermsByGroup.forEachValue(new TObjectProcedure<PriorityQueue<TermStat>>() {
                    @Override
                    public boolean execute(final PriorityQueue<TermStat> topTerms) {
                        termsAndGroups.add(topTerms.size());
                        return true;
                    }
                });

                final TermStat[] topTermsArray = new TermStat[termsAndGroups.intValue()];

                topTermsByGroup.forEachEntry(new TIntObjectProcedure<PriorityQueue<TermStat>>() {
                    private int i = 0;

                    @Override
                    public boolean execute(final int group, final PriorityQueue<TermStat> topTerms) {
                        for (final TermStat term : topTerms) {
                            topTermsArray[i++] = term;
                        }
                        return true;
                    }
                });

                Arrays.sort(topTermsArray, new TermStat.TermGroupComparator());
                topTermsFTGS.addField(fieldName, fieldIsIntType, topTermsArray);
            }

            return new TopTermsFTGSIterator(topTermsFTGS);
        } finally {
            originalIterator.close();
        }
    }

    static class TermStat {
        final boolean fieldIsIntType;
        final long intTerm;
        final String strTerm;
        final long termDocFreq;
        final int group;
        final long stat;
        final long[] groupStats;

        TermStat(final boolean fieldIsIntType, final long intTerm, final String strTerm, final long termDocFreq, final int group, final long stat, final long[] groupStats) {
            this.fieldIsIntType = fieldIsIntType;
            this.intTerm = intTerm;
            this.strTerm = strTerm;
            this.termDocFreq = termDocFreq;
            this.group = group;
            this.stat = stat;
            this.groupStats = groupStats;
        }

        boolean haveSameTerm(final FTGSIteratorUtil.TermStat other) {
            return (fieldIsIntType == other.fieldIsIntType)
                    && (fieldIsIntType ? (intTerm == other.intTerm) : (strTerm.compareTo(other.strTerm) == 0));
        }

        private static class TermGroupComparator implements Comparator<TermStat> {
            @Override
            public int compare(final TermStat x, final TermStat y) {
                final int ret;
                if (x.fieldIsIntType) {
                    ret = Longs.compare(x.intTerm, y.intTerm);
                } else {
                    ret = x.strTerm.compareTo(y.strTerm);
                }

                if (ret != 0) {
                    return ret;
                }
                return Ints.compare(x.group, y.group);
            }
        }

        private static final Comparator<TermStat> TOP_STAT_COMPARATOR = new Comparator<TermStat>() {
            @Override
            public int compare(final TermStat x, final TermStat y) {
                final int ret = Longs.compare(x.stat, y.stat);
                if (ret == 0) {
                    if (x.fieldIsIntType) {
                        return Longs.compare(y.intTerm, x.intTerm);
                    } else {
                        return y.strTerm.compareTo(x.strTerm);
                    }
                }
                return ret;
            }
        };
    }

    static class TopTermsStatsByField {
        static class FieldAndTermStats {
            final String field;
            final boolean isIntType;
            final TermStat[] termStats;

            FieldAndTermStats(final String field, final boolean isIntType, final TermStat[] termStats) {
                this.field = field;
                this.isIntType = isIntType;
                this.termStats = termStats;
            }
        }

        private final List<FieldAndTermStats> fieldTermStatsList = new ArrayList<>();

        void addField(final String field, final boolean isIntType, final TermStat[] terms) {
            fieldTermStatsList.add(new FieldAndTermStats(field, isIntType, terms));
        }

        List<FieldAndTermStats> getEntries() {
            return fieldTermStatsList;
        }
    }

    public static FTGSIterator makeUnsortedIfPossible(final FTGSIterator iterator) {
        if (iterator instanceof SortedFTGSInterleaver) {
            final FTGSIterator[] iterators = ((AbstractDisjointFTGSMerger) iterator).getIterators();
            return new UnsortedFTGSIterator(iterators);
        }
        return iterator;
    }

    // calculate distinct when we know groups count
    public static GroupStatsIterator calculateDistinct(final FTGSIterator iterator, final int numGroups) {
        final FTGSIterator unsortedFtgs = FTGSIteratorUtil.makeUnsortedIfPossible(iterator);

        if (!unsortedFtgs.nextField()) {
            throw new IllegalArgumentException("FTGSIterator with at least one field expected");
        }

        final long[] result = new long[numGroups];
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

    // calculate distinct when we don't know groups count
    public static GroupStatsIterator calculateDistinct(final FTGSIterator iterator) {
        final FTGSIterator unsortedFtgs = FTGSIteratorUtil.makeUnsortedIfPossible(iterator);

        if (!unsortedFtgs.nextField()) {
            throw new IllegalArgumentException("FTGSIterator with at least one field expected");
        }

        long[] result = new long[0];
        while (unsortedFtgs.nextTerm()) {
            while (unsortedFtgs.nextGroup()) {
                final int group = unsortedFtgs.group();
                if (group >= result.length) {
                    result = Arrays.copyOf(result, group + 1);
                }
                result[group]++;
            }
        }

        if (unsortedFtgs.nextField()) {
            throw new IllegalArgumentException("FTGSIterator with exactly one field expected");
        }

        return new GroupStatsDummyIterator(result);
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
}

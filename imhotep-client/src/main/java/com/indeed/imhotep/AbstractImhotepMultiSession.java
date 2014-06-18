package com.indeed.imhotep;

import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.io.Closer;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
//import com.indeed.util.functional.BlockingCopyableIterator;
//import com.indeed.util.Closer;
import com.indeed.util.core.Throwables2;
import com.indeed.util.core.io.Closeables2;
import com.indeed.util.core.threads.LogOnUncaughtExceptionHandler;
import com.indeed.flamdex.query.Term;
import com.indeed.flamdex.utils.BlockingCopyableIterator;
import com.indeed.imhotep.api.DocIterator;
import com.indeed.imhotep.api.FTGSIterator;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.imhotep.api.ImhotepSession;
import com.indeed.imhotep.api.RawFTGSIterator;
import com.indeed.imhotep.io.CircularIOStream;
import com.indeed.imhotep.service.DocIteratorMerger;
import com.indeed.imhotep.service.FTGSOutputStreamWriter;

import it.unimi.dsi.fastutil.longs.Long2LongMap;
import it.unimi.dsi.fastutil.longs.Long2LongOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongIterator;
import it.unimi.dsi.fastutil.objects.Object2LongMap;
import it.unimi.dsi.fastutil.objects.Object2LongOpenHashMap;

import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @author jsgroth
 */
public abstract class AbstractImhotepMultiSession extends AbstractImhotepSession {
    private static final Logger log = Logger.getLogger(AbstractImhotepMultiSession.class);

    public static final int MERGE_BUFFER_SIZE = 65536;

    public static final int SPLIT_BUFFER_SIZE = 16384;

    protected final ImhotepSession[] sessions;

    private final Long[] totalDocFreqBuf;

    private final Integer[] integerBuf;

    private final Object[] nullBuf;

    private final long[][] groupStatsBuf;

    private final List<TermCount>[] termCountListBuf;

    private FTGSIterator lastIterator;

    private final int numSplits;

    private final ExecutorService bufferThreads = Executors.newCachedThreadPool(
            new ThreadFactoryBuilder().setDaemon(true)
                    .setNameFormat("FTGS-Buffer-Thread-%d")
                    .setUncaughtExceptionHandler(new LogOnUncaughtExceptionHandler(log))
                    .build()
    );

    protected int numStats = 0;

    private int numGroups = 2;

    @SuppressWarnings({"unchecked"})
    protected AbstractImhotepMultiSession(ImhotepSession[] sessions, int numSplits) {
        this.numSplits = numSplits;
        if (sessions == null || sessions.length == 0) {
            throw new IllegalArgumentException("at least one session is required");
        }

        this.sessions = sessions;

        totalDocFreqBuf = new Long[sessions.length];
        integerBuf = new Integer[sessions.length];
        nullBuf = new Object[sessions.length];
        groupStatsBuf = new long[sessions.length][];
        termCountListBuf = new List[sessions.length];
    }

    @Override
    public long getTotalDocFreq(final String[] intFields, final String[] stringFields) {
        executeRuntimeException(totalDocFreqBuf, new ThrowingFunction<ImhotepSession, Long>() {
            @Override
            public Long apply(ImhotepSession session) throws Exception {
                return session.getTotalDocFreq(intFields, stringFields);
            }
        });
        long sum = 0L;
        for (final long totalDocFreq : totalDocFreqBuf) {
            sum += totalDocFreq;
        }
        return sum;
    }

    @Override
    public long[] getGroupStats(final int stat) {
        executeRuntimeException(groupStatsBuf, new ThrowingFunction<ImhotepSession, long[]>() {
            @Override
            public long[] apply(ImhotepSession session) throws Exception {
                return session.getGroupStats(stat);
            }
        });

        int numGroups = 0;
        for (int i = 0; i < sessions.length; ++i) {
            final long[] statsBuf = groupStatsBuf[i];
            final int statsBufLength = statsBuf.length;
            numGroups = Math.max(numGroups, statsBufLength);
        }

        final long[] totalStats = new long[numGroups];
        for (final long[] stats : groupStatsBuf) {
            for (int group = 1; group < stats.length; ++group) {
                totalStats[group] += stats[group];
            }
        }
        return totalStats;
    }

    @Override
    public int regroup(final GroupMultiRemapRule[] rawRules, final boolean errorOnCollisions) throws ImhotepOutOfMemoryException {
        executeMemoryException(integerBuf, new ThrowingFunction<ImhotepSession, Integer>() {
            @Override
            public Integer apply(ImhotepSession session) throws Exception {
                return session.regroup(rawRules, errorOnCollisions);
            }
        });

        numGroups = Collections.max(Arrays.asList(integerBuf));
        return numGroups;
    }

    @Override
    public int regroup(final int numRawRules, final Iterator<GroupMultiRemapRule> rawRules, final boolean errorOnCollisions) throws ImhotepOutOfMemoryException {
        final BlockingCopyableIterator<GroupMultiRemapRule> copyableIterator = new BlockingCopyableIterator<GroupMultiRemapRule>(rawRules, sessions.length, 256);

        executeMemoryException(integerBuf, new ThrowingFunction<ImhotepSession, Integer>() {
            @Override
            public Integer apply(ImhotepSession session) throws Exception {
                return session.regroup(numRawRules, copyableIterator.iterator(), errorOnCollisions);
            }
        });

        numGroups = Collections.max(Arrays.asList(integerBuf));
        return numGroups;
    }

    @Override
    public int regroup(final GroupRemapRule[] rawRules) throws ImhotepOutOfMemoryException {
        executeMemoryException(integerBuf, new ThrowingFunction<ImhotepSession, Integer>() {
            @Override
            public Integer apply(ImhotepSession session) throws Exception {
                return session.regroup(rawRules);
            }
        });

        numGroups = Collections.max(Arrays.asList(integerBuf));
        return numGroups;
    }

    public int regroup2(final int numRules, final Iterator<GroupRemapRule> rules) throws ImhotepOutOfMemoryException {
        final BlockingCopyableIterator<GroupRemapRule> copyableIterator = new BlockingCopyableIterator<GroupRemapRule>(rules, sessions.length, 256);
        executeMemoryException(integerBuf, new ThrowingFunction<ImhotepSession, Integer>() {
            @Override
            public Integer apply(ImhotepSession session) throws Exception {
                return session.regroup2(numRules, copyableIterator.iterator());
            }
        });

        numGroups = Collections.max(Arrays.asList(integerBuf));
        return numGroups;
    }

    @Override
    public int regroup(final QueryRemapRule rule) throws ImhotepOutOfMemoryException {
        executeMemoryException(integerBuf, new ThrowingFunction<ImhotepSession, Integer>() {
            @Override
            public Integer apply(ImhotepSession session) throws Exception {
                return session.regroup(rule);
            }
        });

        numGroups = Collections.max(Arrays.asList(integerBuf));
        return numGroups;
    }

    @Override
    public void intOrRegroup(final String field, final long[] terms, final int targetGroup, final int negativeGroup, final int positiveGroup) throws ImhotepOutOfMemoryException {
        executeMemoryException(nullBuf, new ThrowingFunction<ImhotepSession, Object>() {
            @Override
            public Object apply(ImhotepSession session) throws Exception {
                session.intOrRegroup(field, terms, targetGroup, negativeGroup, positiveGroup);
                return null;
            }
        });
    }

    @Override
    public void stringOrRegroup(final String field, final String[] terms, final int targetGroup, final int negativeGroup, final int positiveGroup) throws ImhotepOutOfMemoryException {
        executeMemoryException(nullBuf, new ThrowingFunction<ImhotepSession, Object>() {
            @Override
            public Object apply(ImhotepSession session) throws Exception {
                session.stringOrRegroup(field, terms, targetGroup, negativeGroup, positiveGroup);
                return null;
            }
        });
    }

    @Override
    public void randomRegroup(final String field, final boolean isIntField, final String salt, final double p, final int targetGroup,
                              final int negativeGroup, final int positiveGroup) throws ImhotepOutOfMemoryException {
        executeMemoryException(nullBuf, new ThrowingFunction<ImhotepSession, Object>() {
            @Override
            public Object apply(ImhotepSession session) throws Exception {
                session.randomRegroup(field, isIntField, salt, p, targetGroup, negativeGroup, positiveGroup);
                return null;
            }
        });
    }

    @Override
    public void randomMultiRegroup(final String field, final boolean isIntField, final String salt, final int targetGroup,
                                   final double[] percentages, final int[] resultGroups) throws ImhotepOutOfMemoryException {
        executeMemoryException(nullBuf, new ThrowingFunction<ImhotepSession, Object>() {
            @Override
            public Object apply(ImhotepSession session) throws Exception {
                session.randomMultiRegroup(field, isIntField, salt, targetGroup, percentages, resultGroups);
                return null;
            }
        });
    }

    @Override
    public int metricRegroup(final int stat, final long min, final long max, final long intervalSize) throws ImhotepOutOfMemoryException {
        executeMemoryException(integerBuf, new ThrowingFunction<ImhotepSession, Integer>() {
            @Override
            public Integer apply(ImhotepSession session) throws Exception {
                return session.metricRegroup(stat, min, max, intervalSize);
            }
        });

        numGroups = Collections.max(Arrays.asList(integerBuf));
        return numGroups;
    }

    @Override
    public int metricRegroup2D(final int xStat, final long xMin, final long xMax, final long xIntervalSize,
                               final int yStat, final long yMin, final long yMax, final long yIntervalSize) throws ImhotepOutOfMemoryException {
        executeMemoryException(integerBuf, new ThrowingFunction<ImhotepSession, Integer>() {
            @Override
            public Integer apply(ImhotepSession session) throws Exception {
                return session.metricRegroup2D(xStat, xMin, xMax, xIntervalSize, yStat, yMin, yMax, yIntervalSize);
            }
        });

        numGroups = Collections.max(Arrays.asList(integerBuf));
        return numGroups;
    }

    public int metricFilter(final int stat, final long min, final long max, final boolean negate) throws ImhotepOutOfMemoryException {
        executeMemoryException(integerBuf, new ThrowingFunction<ImhotepSession, Integer>() {
            @Override
            public Integer apply(ImhotepSession session) throws Exception {
                return session.metricFilter(stat, min, max, negate);
            }
        });

        numGroups = Collections.max(Arrays.asList(integerBuf));
        return numGroups;
    }

    @Override
    public List<TermCount> approximateTopTerms(final String field, final boolean isIntField, final int k) {
        final int subSessionK = k * 2;

        executeRuntimeException(termCountListBuf, new ThrowingFunction<ImhotepSession, List<TermCount>>() {
            @Override
            public List<TermCount> apply(ImhotepSession session) throws Exception {
                return session.approximateTopTerms(field, isIntField, subSessionK);
            }
        });

        return mergeTermCountLists(termCountListBuf, field, isIntField, k);
    }

    private static List<TermCount> mergeTermCountLists(List<TermCount>[] termCountListBuf, String field, boolean isIntField, int k) {
        final List<TermCount> ret;
        if (isIntField) {
            final Long2LongMap counts = new Long2LongOpenHashMap(k * 2);
            for (final List<TermCount> list : termCountListBuf) {
                for (final TermCount termCount : list) {
                    final long termVal = termCount.getTerm().getTermIntVal();
                    final long count = termCount.getCount();
                    counts.put(termVal, counts.get(termVal) + count);
                }
            }
            ret = Lists.newArrayListWithCapacity(counts.size());
            for (final LongIterator iter = counts.keySet().iterator(); iter.hasNext(); ) {
                final Long term = iter.nextLong();
                final long count = counts.get(term);
                ret.add(new TermCount(new Term(field, isIntField, term, ""), count));
            }
        } else {
            final Object2LongMap<String> counts = new Object2LongOpenHashMap<String>(k * 2);
            for (final List<TermCount> list : termCountListBuf) {
                for (final TermCount termCount : list) {
                    final String termVal = termCount.getTerm().getTermStringVal();
                    final long count = termCount.getCount();
                    counts.put(termVal, counts.getLong(termVal) + count);
                }
            }
            ret = Lists.newArrayListWithCapacity(counts.size());
            for (final String term : counts.keySet()) {
                final long count = counts.get(term);
                ret.add(new TermCount(new Term(field, isIntField, 0, term), count));
            }
        }
        Collections.sort(ret, TermCount.REVERSE_COUNT_COMPARATOR);
        final int end = Math.min(k, ret.size());
        return ret.subList(0, end);
    }

    @Override
    public int pushStat(final String statName) throws ImhotepOutOfMemoryException {
        executeMemoryException(integerBuf, new ThrowingFunction<ImhotepSession, Integer>() {
            @Override
            public Integer apply(ImhotepSession session) throws Exception {
                return session.pushStat(statName);
            }
        });

        numStats = validateNumStats(integerBuf);
        return numStats;
    }

    @Override
    public int pushStats(final List<String> statNames) throws ImhotepOutOfMemoryException {
        executeRuntimeException(integerBuf, new ThrowingFunction<ImhotepSession, Integer>() {
            @Override
            public Integer apply(ImhotepSession session) throws Exception {
                return session.pushStats(statNames);
            }
        });

        numStats = validateNumStats(integerBuf);
        return numStats;
    }

    @Override
    public int popStat() {
        executeRuntimeException(integerBuf, new ThrowingFunction<ImhotepSession, Integer>() {
            @Override
            public Integer apply(ImhotepSession session) throws Exception {
                return session.popStat();
            }
        });

        numStats = validateNumStats(integerBuf);
        return numStats;
    }

    @Override
    public int getNumStats() {
        executeRuntimeException(integerBuf, new ThrowingFunction<ImhotepSession, Integer>() {
            @Override
            public Integer apply(ImhotepSession session) throws Exception {
                return session.getNumStats();
            }
        });

        numStats = validateNumStats(integerBuf);
        return numStats;
    }

    @Override
    public void createDynamicMetric(final String name) throws ImhotepOutOfMemoryException {
        executeRuntimeException(nullBuf, new ThrowingFunction<ImhotepSession, Object>() {
            @Override
            public Object apply(ImhotepSession imhotepSession) throws Exception {
                imhotepSession.createDynamicMetric(name);
                return null;
            }
        });
    }

    @Override
    public void updateDynamicMetric(final String name, final int[] deltas) {
        executeRuntimeException(nullBuf, new ThrowingFunction<ImhotepSession, Object>() {
            @Override
            public Object apply(ImhotepSession imhotepSession) throws Exception {
                imhotepSession.updateDynamicMetric(name, deltas);
                return null;
            }
        });
    }

    @Override
    public void conditionalUpdateDynamicMetric(final String name, final RegroupCondition[] conditions, final int[] deltas) {
        executeRuntimeException(nullBuf, new ThrowingFunction<ImhotepSession, Object>() {
            @Override
            public Object apply(ImhotepSession imhotepSession) throws Exception {
                imhotepSession.conditionalUpdateDynamicMetric(name, conditions, deltas);
                return null;
            }
        });
    }

    private static int validateNumStats(final Integer[] numStatBuf) {
        final int newNumStats = numStatBuf[0];
        for (int i = 1; i < numStatBuf.length; ++i) {
            if (numStatBuf[i] != newNumStats) {
                throw new RuntimeException("bug, one session did not return the same number of stats as the others");
            }
        }
        return newNumStats;
    }

    @Override
    public FTGSIterator getFTGSIterator(final String[] intFields, final String[] stringFields) {
        if (lastIterator != null) {
            Closeables2.closeQuietly(lastIterator, log);
            lastIterator = null;
        }
        if (sessions.length == 1) {
            lastIterator = sessions[0].getFTGSIterator(intFields, stringFields);
            return lastIterator;
        }
        final Closer closer = Closer.create();
        try {
            final List<RawFTGSIterator> mergers = getIteratorSplits0(intFields, stringFields, numSplits, true);
            for (RawFTGSIterator iter : mergers) {
                closer.register(iter);
            }
            final RawFTGSMerger finalMerger = new RawFTGSMerger(mergers, numStats, null);
            lastIterator = new BufferedFTGSIterator(bufferThreads, finalMerger, numStats, false);
            return lastIterator;
        } catch (Throwable t) {
            Closeables2.closeQuietly(closer, log);
            throw Throwables.propagate(t);
        }
    }

    public final DocIterator getDocIterator(String[] intFields, String[] stringFields) throws ImhotepOutOfMemoryException {
        final Closer closer = Closer.create();
        try {
            final List<DocIterator> docIterators = Lists.newArrayList();
            for (ImhotepSession session : sessions) {
                docIterators.add(closer.register(session.getDocIterator(intFields, stringFields)));
            }
            return new DocIteratorMerger(docIterators, intFields.length, stringFields.length);
        } catch (Throwable t) {
            Closeables2.closeQuietly(closer, log);
            throw Throwables2.propagate(t, ImhotepOutOfMemoryException.class);
        }
    }

    private List<RawFTGSIterator> getIteratorSplits0(final String[] intFields, final String[] stringFields, final int numSplits, boolean writeEmptyTerms) throws IOException {
        if (sessions.length == 1) {
            return getSplitsSingleSession(intFields, stringFields, numSplits);
        }
        final Closer closer = Closer.create();
        try {
            final RawFTGSIterator[][] iteratorSplits = new RawFTGSIterator[sessions.length][];
            for (int i = 0; i < sessions.length; i++) {
                final FTGSSplitter splitter = closer.register(new FTGSSplitter(sessions[i].getFTGSIterator(intFields, stringFields), numSplits, numStats));
                iteratorSplits[i] = splitter.getFtgsIterators();
            }
            final List<RawFTGSIterator> mergers = Lists.newArrayList();
            for (int j = 0; j < numSplits; j++) {
                final List<RawFTGSIterator> iterators = Lists.newArrayList();
                for (int i = 0; i < sessions.length; i++) {
                    iterators.add(iteratorSplits[i][j]);
                }

                final RawFTGSMerger rawFTGSMerger = new RawFTGSMerger(iterators, numStats, null);
                mergers.add(closer.register(new BufferedFTGSIterator(bufferThreads, rawFTGSMerger, numStats, writeEmptyTerms)));
            }
            return mergers;
        } catch (Throwable t) {
            Closeables2.closeQuietly(closer, log);
            throw Throwables2.propagate(t, IOException.class);
        }
    }

    private List<RawFTGSIterator> getSplitsSingleSession(final String[] intFields, final String[] stringFields, final int numSplits) {
        final Closer closer = Closer.create();
        try {
            final FTGSSplitter splitter = closer.register(new FTGSSplitter(sessions[0].getFTGSIterator(intFields, stringFields), numSplits, numStats));
            final List<RawFTGSIterator> mergers = Lists.newArrayList();
            for (RawFTGSIterator iterator : splitter.getFtgsIterators()) {
                mergers.add(closer.register(new BufferedFTGSIterator(bufferThreads, iterator, numStats, false)));
            }
            return mergers;
        } catch (Throwable t) {
            Closeables2.closeQuietly(closer, log);
            throw Throwables.propagate(t);
        }
    }

    public RawFTGSIterator[] getFTGSIteratorSplits(final String[] intFields, final String[] stringFields, final int numSplits) {
        try {
            final List<RawFTGSIterator> mergers = getIteratorSplits0(intFields, stringFields, numSplits, false);
            return mergers.toArray(new RawFTGSIterator[mergers.size()]);
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public void rebuildAndFilterIndexes(final List<String> intFields, 
                                final List<String> stringFields) throws ImhotepOutOfMemoryException {
        executeMemoryException(nullBuf, new ThrowingFunction<ImhotepSession, Object>() {
            @Override
            public Object apply(ImhotepSession imhotepSession) throws Exception {
                imhotepSession.rebuildAndFilterIndexes(intFields, stringFields);
                return null;
          }
        });
    }
    
    @Override
    public final void close() {
        try {
            preClose();
        } finally {
            try {
                executeRuntimeException(nullBuf, new ThrowingFunction<ImhotepSession, Object>() {
                    @Override
                    public Object apply(ImhotepSession imhotepSession) throws Exception {
                        imhotepSession.close();
                        return null;
                    }
                });
            } finally {
                postClose();
            }
        }
    }

    @Override
    public void resetGroups() {
        executeRuntimeException(nullBuf, new ThrowingFunction<ImhotepSession, Object>() {
            @Override
            public Object apply(ImhotepSession imhotepSession) throws Exception {
                imhotepSession.resetGroups();
                return null;
            }
        });
    }

    protected void preClose() {
        try {
            if (lastIterator != null) {
                Closeables2.closeQuietly(lastIterator, log);
                lastIterator = null;
            }
        } finally {
            bufferThreads.shutdown();
        }
    }

    protected abstract void postClose();

    protected <T> void executeRuntimeException(final T[] ret, final ThrowingFunction<? super ImhotepSession, ? extends T> function) {
        try {
            execute(ret, function);
        } catch (ExecutionException e) {
            throw new RuntimeException(e.getCause());
        }
    }

    protected <T> void executeMemoryException(final T[] ret, final ThrowingFunction<? super ImhotepSession, ? extends T> function) throws ImhotepOutOfMemoryException {
        try {
            execute(ret, function);
        } catch (ExecutionException e) {
            final Throwable cause = e.getCause();
            if (cause instanceof ImhotepOutOfMemoryException) {
                throw new ImhotepOutOfMemoryException(cause);
            } else {
                throw new RuntimeException(cause);
            }
        }
    }

    protected abstract <T> void execute(final T[] ret, final ThrowingFunction<? super ImhotepSession, ? extends T> function) throws ExecutionException;

    protected static interface ThrowingFunction<K, V> {
        V apply(K k) throws Exception;
    }

    protected void safeClose() {
        try {
            close();
        } catch (Exception e) {
            log.error("error closing session", e);
        }
    }

    private static final class BufferedFTGSIterator implements RawFTGSIterator {
        private final InputStreamFTGSIterator iterator;
        private final FTGSOutputStreamWriter writer;
        private final CircularIOStream buffer;

        private final RawFTGSIterator rawIterator;

        private BufferedFTGSIterator(ExecutorService threadPool, final RawFTGSIterator rawIterator, final int numStats, boolean writeEmptyTerms) throws IOException {
            this.rawIterator = rawIterator;
            buffer = new CircularIOStream(MERGE_BUFFER_SIZE);
            writer = new FTGSOutputStreamWriter(buffer.getOutputStream(), writeEmptyTerms);
            iterator = new InputStreamFTGSIterator(buffer.getInputStream(), numStats);
            threadPool.submit(new Runnable() {
                @Override
                public void run() {
                    try {
                        writer.write(rawIterator, numStats);
                    } catch (IOException e) {
                        throw Throwables.propagate(e);
                    } catch (Throwable t) {
                        log.error("error", t);
                        throw Throwables.propagate(t);
                    } finally {
                        Closeables2.closeAll(log, buffer.getOutputStream(), rawIterator);
                    }
                }
            });
        }

        @Override
        public boolean nextField() {
            return iterator.nextField();
        }

        @Override
        public String fieldName() {
            return iterator.fieldName();
        }

        @Override
        public boolean fieldIsIntType() {
            return iterator.fieldIsIntType();
        }

        @Override
        public boolean nextTerm() {
            return iterator.nextTerm();
        }

        @Override
        public long termDocFreq() {
            return iterator.termDocFreq();
        }

        @Override
        public long termIntVal() {
            return iterator.termIntVal();
        }

        @Override
        public String termStringVal() {
            return iterator.termStringVal();
        }

        @Override
        public byte[] termStringBytes() {
            return iterator.termStringBytes();
        }

        @Override
        public int termStringLength() {
            return iterator.termStringLength();
        }

        @Override
        public boolean nextGroup() {
            return iterator.nextGroup();
        }

        @Override
        public int group() {
            return iterator.group();
        }

        @Override
        public void groupStats(final long[] stats) {
            iterator.groupStats(stats);
        }

        @Override
        public void close() {
            Closeables2.closeAll(log, rawIterator, iterator, buffer.getOutputStream());
        }
    }
}

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
package com.indeed.imhotep.local;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimaps;
import com.google.common.io.Closer;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import com.google.protobuf.InvalidProtocolBufferException;
import com.indeed.flamdex.api.DocIdStream;
import com.indeed.flamdex.api.FlamdexOutOfMemoryException;
import com.indeed.flamdex.api.FlamdexReader;
import com.indeed.flamdex.api.IntTermIterator;
import com.indeed.flamdex.api.IntValueLookup;
import com.indeed.flamdex.api.StringTermDocIterator;
import com.indeed.flamdex.api.StringTermIterator;
import com.indeed.flamdex.api.TermIterator;
import com.indeed.flamdex.datastruct.FastBitSet;
import com.indeed.flamdex.datastruct.FastBitSetPooler;
import com.indeed.flamdex.fieldcache.ByteArrayIntValueLookup;
import com.indeed.flamdex.fieldcache.CharArrayIntValueLookup;
import com.indeed.flamdex.fieldcache.IntArrayIntValueLookup;
import com.indeed.flamdex.query.Query;
import com.indeed.flamdex.query.Term;
import com.indeed.flamdex.search.FlamdexSearcher;
import com.indeed.flamdex.utils.FlamdexUtils;
import com.indeed.imhotep.AbstractImhotepSession;
import com.indeed.imhotep.EmptyFTGSIterator;
import com.indeed.imhotep.FTGSIteratorUtil;
import com.indeed.imhotep.FTGSSplitter;
import com.indeed.imhotep.GroupMultiRemapRule;
import com.indeed.imhotep.GroupRemapRule;
import com.indeed.imhotep.GroupStatsDummyIterator;
import com.indeed.imhotep.ImhotepMemoryPool;
import com.indeed.imhotep.Instrumentation;
import com.indeed.imhotep.MemoryReservationContext;
import com.indeed.imhotep.QueryRemapRule;
import com.indeed.imhotep.RegroupCondition;
import com.indeed.imhotep.TermCount;
import com.indeed.imhotep.TermLimitedFTGSIterator;
import com.indeed.imhotep.api.FTGSIterator;
import com.indeed.imhotep.api.FTGSParams;
import com.indeed.imhotep.api.GroupStatsIterator;
import com.indeed.imhotep.api.ImhotepCommand;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.imhotep.api.ImhotepSession;
import com.indeed.imhotep.api.PerformanceStats;
import com.indeed.imhotep.api.RegroupParams;
import com.indeed.imhotep.exceptions.MultiValuedFieldStringLenException;
import com.indeed.imhotep.group.IterativeHasher;
import com.indeed.imhotep.group.IterativeHasherUtils;
import com.indeed.imhotep.marshal.ImhotepDaemonMarshaller;
import com.indeed.imhotep.matcher.StringTermMatcher;
import com.indeed.imhotep.matcher.StringTermMatchers;
import com.indeed.imhotep.metrics.AbsoluteValue;
import com.indeed.imhotep.metrics.Addition;
import com.indeed.imhotep.metrics.CachedInterleavedMetrics;
import com.indeed.imhotep.metrics.CachedMetric;
import com.indeed.imhotep.metrics.Constant;
import com.indeed.imhotep.metrics.Count;
import com.indeed.imhotep.metrics.DelegatingMetric;
import com.indeed.imhotep.metrics.Division;
import com.indeed.imhotep.metrics.DocIdMetric;
import com.indeed.imhotep.metrics.Equal;
import com.indeed.imhotep.metrics.Exponential;
import com.indeed.imhotep.metrics.GreaterThan;
import com.indeed.imhotep.metrics.GreaterThanOrEqual;
import com.indeed.imhotep.metrics.LessThan;
import com.indeed.imhotep.metrics.LessThanOrEqual;
import com.indeed.imhotep.metrics.Log;
import com.indeed.imhotep.metrics.Log1pExp;
import com.indeed.imhotep.metrics.Logistic;
import com.indeed.imhotep.metrics.Max;
import com.indeed.imhotep.metrics.Min;
import com.indeed.imhotep.metrics.Modulus;
import com.indeed.imhotep.metrics.Multiplication;
import com.indeed.imhotep.metrics.NotEqual;
import com.indeed.imhotep.metrics.ShiftLeft;
import com.indeed.imhotep.metrics.ShiftRight;
import com.indeed.imhotep.metrics.Subtraction;
import com.indeed.imhotep.pool.BuffersPool;
import com.indeed.imhotep.protobuf.Operator;
import com.indeed.imhotep.protobuf.QueryMessage;
import com.indeed.imhotep.service.InstrumentedFlamdexReader;
import com.indeed.util.core.Pair;
import com.indeed.util.core.Throwables2;
import com.indeed.util.core.io.Closeables2;
import com.indeed.util.core.reference.SharedReference;
import com.indeed.util.core.threads.ThreadSafeBitSet;
import it.unimi.dsi.fastutil.PriorityQueue;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntIterator;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectHeapPriorityQueue;
import org.apache.commons.codec.binary.Base64;
import org.apache.log4j.Logger;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * This class isn't even close to remotely thread safe, do not use it
 * simultaneously from multiple threads
 */
public abstract class ImhotepLocalSession extends AbstractImhotepSession {

    static final Logger log = Logger.getLogger(ImhotepLocalSession.class);

    static final boolean logTiming;

    static {
        logTiming =
            "true".equals(System.getProperty("com.indeed.imhotep.local.ImhotepLocalSession.logTiming"));
    }

    static final int MAX_NUMBER_STATS = 64;
    public static final int BUFFER_SIZE = 2048;
    private final AtomicLong tempFileSizeBytesLeft;
    private long savedTempFileSizeValue;
    private PerformanceStats resetPerformanceStats = new PerformanceStats(0, 0, 0, 0, 0, 0, 0, 0, ImmutableMap.of());

    protected int numDocs;

    // TODO: buffers pool should be shared across sessions.
    final BuffersPool memoryPool = new BuffersPool();

    // do not close flamdexReader, it is separately refcounted
    protected FlamdexReader flamdexReader;
    protected SharedReference<FlamdexReader> flamdexReaderRef;

    private final InstrumentedFlamdexReader instrumentedFlamdexReader;

    @Nullable
    private final Interval shardTimeRange;

    final MemoryReservationContext memory;

    protected NamedGroupManager namedGroupLookups;

    protected final MetricStack metricStack = new MetricStack();

    private boolean closed = false;

    @VisibleForTesting
    protected Map<String, DynamicMetric> dynamicMetrics = Maps.newHashMap();

    private final Exception constructorStackTrace;

    class CloseLocalSessionEvent extends Instrumentation.Event {
        CloseLocalSessionEvent() {
            super(CloseLocalSessionEvent.class.getSimpleName());
            getProperties()
                .putAll(ImhotepLocalSession.this.instrumentedFlamdexReader.sample().getProperties());
            getProperties().put(Instrumentation.Keys.MAX_USED_MEMORY,
                                ImhotepLocalSession.this.memory.getGlobalMaxUsedMemory());
        }
    }

    public ImhotepLocalSession(final String sessionId, final FlamdexReader flamdexReader, @Nullable final Interval shardTimeRange)
        throws ImhotepOutOfMemoryException {
        this(sessionId, flamdexReader, shardTimeRange, new MemoryReservationContext(new ImhotepMemoryPool(Long.MAX_VALUE)), null);
    }

    public ImhotepLocalSession(final String sessionId,
                               final FlamdexReader flamdexReader,
                               @Nullable final Interval shardTimeRange,
                               final MemoryReservationContext memory,
                               final AtomicLong tempFileSizeBytesLeft)
        throws ImhotepOutOfMemoryException {
        super(sessionId);
        this.tempFileSizeBytesLeft = tempFileSizeBytesLeft;
        this.shardTimeRange = shardTimeRange;
        this.savedTempFileSizeValue = (this.tempFileSizeBytesLeft == null) ? 0 : this.tempFileSizeBytesLeft.get();
        constructorStackTrace = new Exception();
        this.instrumentedFlamdexReader = new InstrumentedFlamdexReader(flamdexReader);
        this.flamdexReader = this.instrumentedFlamdexReader; // !@# remove this alias
        this.flamdexReaderRef = SharedReference.create(this.flamdexReader);
        this.memory = memory;
        this.numDocs = flamdexReader.getNumDocs();

        namedGroupLookups = new NamedGroupManager(sessionId, memory);
        resetGroupsTo(ImhotepSession.DEFAULT_GROUPS, 1);

        this.metricStack.addObserver(new StatLookup.Observer() {
                public void onChange(final StatLookup statLookup, final int index) {
                    instrumentedFlamdexReader.onPushStat(statLookup.getName(index),
                                                         statLookup.get(index));
                }
            });
    }

    private GroupLookup moveDeletedDocumentsToGroupZero(GroupLookup result) throws ImhotepOutOfMemoryException {
        final IntIterator deletedDocIds = flamdexReader.getDeletedDocIterator();
        if (deletedDocIds.hasNext()) {
            result = GroupLookupFactory.resize(result, result.getNumGroups(), memory);
            while (deletedDocIds.hasNext()) {
                result.set(deletedDocIds.nextInt(), 0);
            }
        }
        result.recalculateNumGroups();
        return result;
    }

    FlamdexReader getReader() {
        return this.flamdexReader;
    }

    @Nullable
    @Override
    public Path getShardPath() {
        return this.flamdexReader.getDirectory();
    }

    /**
     * Read numStats without providing any guarantees about reading
     * the most up to date value
     */
    @Override
    public int weakGetNumStats() {
        return this.metricStack.getNumStats();
    }

    /**
     * Read numGroups without providing any guarantees about reading
     * the most up to date value
     */
    @Override
    public Map<String, Integer> weakGetNumGroups(final String groupsName) {
        return namedGroupLookups.getAllNumGroups();
    }

    public Map<String, DynamicMetric> getDynamicMetrics() {
        return dynamicMetrics;
    }

    public long getNumDocs() {
        return this.numDocs;
    }

    @Override
    public synchronized PerformanceStats getPerformanceStats(final boolean reset) {
        final InstrumentedFlamdexReader.PerformanceStats flamdexPerformanceStats = instrumentedFlamdexReader.getPerformanceStats();
        final long fieldFilesReadSize = flamdexPerformanceStats.fieldFilesReadSize;
        final long metricsMemorySize = flamdexPerformanceStats.metricsMemorySize;
        final long tempFileSize = (tempFileSizeBytesLeft == null) ? 0 : tempFileSizeBytesLeft.get();
        final long cpuTime = 0;
        final PerformanceStats result =
                new PerformanceStats(
                        cpuTime - resetPerformanceStats.cpuTime,
                        memory.getCurrentMaxUsedMemory() + metricsMemorySize,
                        savedTempFileSizeValue - tempFileSize,
                        fieldFilesReadSize - resetPerformanceStats.fieldFilesReadSize,
                        0, 0, 0, 0, ImmutableMap.of());
        if (reset) {
            resetPerformanceStats = result;
            memory.resetCurrentMaxUsedMemory();
            savedTempFileSizeValue = tempFileSize;
        }
        return result;
    }

    @Override
    public synchronized PerformanceStats closeAndGetPerformanceStats() {
        if(closed) {
            return null;
        }

        final PerformanceStats stats = getPerformanceStats(false);
        close();
        return stats;
    }

    /**
     * export the current docId -&gt; group lookup into an array
     *
     * @param array
     *            the array to export docIdToGroup into
     */
    @VisibleForTesting
    public void exportDocIdToGroupId(final int[] array) {
        exportDocIdToGroupId(ImhotepSession.DEFAULT_GROUPS, array);
    }

    @VisibleForTesting
    public synchronized void exportDocIdToGroupId(final String groupsName, final int[] array) {
        final GroupLookup docIdToGroup = namedGroupLookups.get(groupsName);
        if (array.length != docIdToGroup.size()) {
            throw newIllegalArgumentException("array length is invalid");
        }
        for (int i = array.length - 1; i >= 0; --i) {
            array[i] = docIdToGroup.get(i);
        }
    }

    public boolean isFilteredOut(final String groupsName) {
        return namedGroupLookups.get(groupsName).isFilteredOut();
    }

    @Override
    public synchronized long getTotalDocFreq(final String[] intFields, final String[] stringFields) {
        long ret = 0L;

        for (final String intField : intFields) {
            ret += flamdexReader.getIntTotalDocFreq(intField);
        }

        for (final String stringField : stringFields) {
            ret += flamdexReader.getStringTotalDocFreq(stringField);
        }

        return ret;
    }

    /**
     * Add stats to array
     * @param docIdToGroup
     * @param stat - stat to add
     * @param partialResult - array with some data. Don't reassign values with new stats result.
     */
    protected abstract void addGroupStats(final GroupLookup docIdToGroup, List<String> stat, long[] partialResult) throws ImhotepOutOfMemoryException;

    @Override
    public synchronized long[] getGroupStats(final String groupsName, final List<String> stat) throws ImhotepOutOfMemoryException {
        return getGroupStatsMulti(groupsName, Collections.singletonList(stat))[0];
    }

    // @Override
    public synchronized long[][] getGroupStatsMulti(final String groupsName, final List<List<String>> stats) throws ImhotepOutOfMemoryException {
        final long[][] result = new long[stats.size()][];

        final GroupLookup docIdToGroup = namedGroupLookups.get(groupsName);
        if (docIdToGroup.isFilteredOut()) {
            Arrays.setAll(result, x -> new long[0]);
            return result;
        }

        for (int i = 0; i < stats.size(); i++) {
            final List<String> stat = stats.get(i);
            result[i] = new long[getNumGroups()];
            addGroupStats(docIdToGroup, stat, result[i]);
        }

        return result;
    }

    @Override
    public synchronized GroupStatsIterator getGroupStatsIterator(final String groupsName, final List<String> stat) throws ImhotepOutOfMemoryException {
        return new GroupStatsDummyIterator(getGroupStats(groupsName, stat));
    }

    @Override
    public synchronized FTGSIterator getFTGSIterator(final String groupsName, final FTGSParams params) throws ImhotepOutOfMemoryException {
        final GroupLookup docIdToGroup = namedGroupLookups.get(groupsName);
        if (docIdToGroup.isFilteredOut()) {
            return new EmptyFTGSIterator(params.intFields, params.stringFields, (params.stats == null) ? metricStack.getNumStats() : params.stats.size());
        }

        final Closer closeOnFailCloser = Closer.create();
        try {
            final MetricStack stack = closeOnFailCloser.register(fromStatsOrStackCopy(params.stats));

            // TODO: support unsorted FlamdexFTGSIterator
            // if params.isTopTerms() then Flamdex iterator can be unsorted
            // We could benefit in case of int/string field conversions
            FTGSIterator iterator = closeOnFailCloser.register(new FlamdexFTGSIterator(
                    this,
                    docIdToGroup,
                    flamdexReaderRef.copy(),
                    params.intFields,
                    params.stringFields,
                    stack
            ));

            if (params.isTopTerms()) {
                iterator = FTGSIteratorUtil.getTopTermsFTGSIterator(iterator, params.termLimit, params.sortStat, params.statsSortOrder);
            } else if (params.isTermLimit()) {
                iterator = new TermLimitedFTGSIterator(iterator, params.termLimit);
            }

            return iterator;
        } catch (final Exception e) {
            Closeables2.closeQuietly(closeOnFailCloser, log);
            throw e;
        }
    }

    @Override
    public FTGSIterator getSubsetFTGSIterator(
            final String groupsName,
            final Map<String, long[]> intFields,
            final Map<String, String[]> stringFields,
            @Nullable final List<List<String>> stats
    ) throws ImhotepOutOfMemoryException {
        final GroupLookup docIdToGroup = namedGroupLookups.get(groupsName);
        if (docIdToGroup.isFilteredOut()) {
            final List<String> intFieldsNames = new ArrayList<>(intFields.size());
            intFields.entrySet().iterator().forEachRemaining(entry -> intFieldsNames.add(entry.getKey()));
            final List<String> strFieldsNames = new ArrayList<>(stringFields.size());
            stringFields.entrySet().iterator().forEachRemaining(entry -> strFieldsNames.add(entry.getKey()));
            return new EmptyFTGSIterator(intFieldsNames.toArray(new String[0]), strFieldsNames.toArray(new String[0]), (stats == null) ? metricStack.getNumStats() : stats.size());
        }
        final MetricStack stack = fromStatsOrStackCopy(stats);
        return new FlamdexSubsetFTGSIterator(this, docIdToGroup, flamdexReaderRef.copy(), intFields, stringFields, stack);
    }

    public FTGSSplitter getFTGSIteratorSplitter(
            final String groupsName,
            final String[] intFields,
            final String[] stringFields,
            final int numSplits,
            final long termLimit,
            @Nullable final List<List<String>> stats) throws ImhotepOutOfMemoryException {
        checkSplitParams(numSplits);
        try {
            return new FTGSSplitter(getFTGSIterator(groupsName, intFields, stringFields, termLimit, stats),
                    numSplits,
                    969168349, tempFileSizeBytesLeft);
        } catch (final IOException e) {
            throw Throwables.propagate(e);
        }
    }

    public  FTGSSplitter getSubsetFTGSIteratorSplitter(
            final String groupsName,
            final Map<String, long[]> intFields,
            final Map<String, String[]> stringFields,
            final int numSplits,
            @Nullable final List<List<String>> stats) throws ImhotepOutOfMemoryException {
        checkSplitParams(numSplits);
        try {
            return new FTGSSplitter(getSubsetFTGSIterator(groupsName, intFields, stringFields, stats),
                    numSplits,
                    969168349, tempFileSizeBytesLeft);
        } catch (final IOException e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public GroupStatsIterator getDistinct(final String groupsName, final String field, final boolean isIntField) {
        final GroupLookup docIdToGroup = namedGroupLookups.get(groupsName);
        if (docIdToGroup.isFilteredOut()) {
            return new GroupStatsDummyIterator(new long[0]);
        }

        final String[] intFields = isIntField ? new String[]{field} : new String[0];
        final String[] strFields = isIntField ? new String[0] : new String[]{field};
        final FTGSIterator iterator;
        try {
            iterator = getFTGSIterator(groupsName, intFields, strFields, Collections.emptyList());
        } catch (ImhotepOutOfMemoryException e) {
            throw new IllegalStateException("Did not expect ftgs with 0 stats to allocate memory!", e);
        }
        return FTGSIteratorUtil.calculateDistinct(iterator);
    }

    @Override
    public synchronized int regroup(final RegroupParams regroupParams,
                                    final GroupMultiRemapRule[] rawRules,
                                    final boolean errorOnCollisions)
        throws ImhotepOutOfMemoryException {

        if (namedGroupLookups.handleFiltered(regroupParams)) {
            return 1;
        }

        final GroupLookup inputGroups = namedGroupLookups.get(regroupParams.getInputGroups());

        final int numRules = rawRules.length;
        if (numRules == 0) {
            resetGroupsTo(regroupParams.getOutputGroups(), 0);
            return inputGroups.getNumGroups();
        }

        final int numConditions = GroupMultiRemapRules.countRemapConditions(rawRules);
        final int highestTarget;
        final int targetGroupBytes = Math.max(numRules * 4, numConditions * 8);
        if (!memory.claimMemory(targetGroupBytes)) {
            throw newImhotepOutOfMemoryException();
        }
        try {
            highestTarget = GroupMultiRemapRules.validateTargets(rawRules);
            GroupMultiRemapRules.validateEqualitySplits(rawRules);
        } finally {
            memory.releaseMemory(targetGroupBytes);
        }

        final int maxIntermediateGroup = Math.max(inputGroups.getNumGroups(), highestTarget);
        final int maxNewGroup = GroupMultiRemapRules.findMaxGroup(rawRules);

        final GroupLookup docIdToGroup = namedGroupLookups.ensureWriteable(regroupParams, Math.max(maxIntermediateGroup, maxNewGroup));

        MultiRegroupInternals.moveUntargeted(docIdToGroup, maxIntermediateGroup, rawRules);

        final int maxConditionIndex = GroupMultiRemapRules.findMaxIntermediateGroup(rawRules);
        final int placeholderGroup = maxConditionIndex + 1;

        final int parallelArrayBytes = 3 * 4 * numConditions + 8 * numConditions;
        // int[highestTarget+1], int[highestTarget+1][], <int or string>[highestTarget+1][]
        // The last two are jagged arrays, and the cardinality of the subarrays
        // sums to numConditions at most
        final int maxInequalityBytes = (highestTarget + 1) * (4 + 8 + 8) + numConditions * (4 + 8);
        final int maxBarrierIndexBytes = numConditions * 4;
        final int remappingBytes = (maxIntermediateGroup + 1) * 4;
        final int totalInternalRegroupBytes =
                parallelArrayBytes + maxInequalityBytes + maxBarrierIndexBytes + remappingBytes;
        final GroupLookup newDocIdToGroup = newGroupLookupWithPlaceholders(docIdToGroup, placeholderGroup);

        try {
            if (!memory.claimMemory(totalInternalRegroupBytes)) {
                throw newImhotepOutOfMemoryException();
            }
            final int[] docIdBuf = memoryPool.getIntBuffer(BUFFER_SIZE, true);
            try {
                MultiRegroupInternals.internalMultiRegroup(docIdToGroup,
                                                           newDocIdToGroup,
                                                           docIdBuf,
                                                           flamdexReader,
                                                           rawRules,
                                                           highestTarget,
                                                           numConditions,
                                                           placeholderGroup,
                                                           maxIntermediateGroup,
                                                           errorOnCollisions);
            } catch (final IOException ex) {
                throw newRuntimeException(ex);
            } finally {
                memoryPool.returnIntBuffer(docIdBuf);
                memory.releaseMemory(totalInternalRegroupBytes);
            }

            final int targetGroupToRuleBytes =
                    Math.max(highestTarget + 1, docIdToGroup.getNumGroups()) * 8;
            if (!memory.claimMemory(targetGroupToRuleBytes)) {
                throw newImhotepOutOfMemoryException();
            }
            try {
                MultiRegroupInternals.internalMultiRegroupCleanup(docIdToGroup,
                                                                  docIdToGroup.getNumGroups(),
                                                                  rawRules,
                                                                  highestTarget,
                                                                  newDocIdToGroup,
                                                                  placeholderGroup);
            } finally {
                memory.releaseMemory(targetGroupToRuleBytes);
            }
        } finally {
            memory.releaseMemory(newDocIdToGroup.memoryUsed());
        }

        return namedGroupLookups.finalizeRegroup(regroupParams);
    }

    // Makes a new GroupLookup with all documents having a nonzero group in the
    // current docIdToGroup
    // having a group of placeholderGroup.
    private synchronized GroupLookup newGroupLookupWithPlaceholders(final GroupLookup docIdToGroup, final int placeholderGroup)
        throws ImhotepOutOfMemoryException {
        final GroupLookup newLookup;

        newLookup = GroupLookupFactory.create(placeholderGroup, docIdToGroup.size(), memory);

        for (int i = 0; i < newLookup.size(); i++) {
            if (docIdToGroup.get(i) != 0) {
                newLookup.set(i, placeholderGroup);
            }
        }
        return newLookup;
    }

    @Override
    public synchronized int regroup(final RegroupParams regroupParams, final GroupRemapRule[] rawRules)
        throws ImhotepOutOfMemoryException {

        if (namedGroupLookups.handleFiltered(regroupParams)) {
            return 1;
        }

        final int requiredMemory = numDocs / 8 + 1;
        if (!memory.claimMemory(requiredMemory)) {
            throw newImhotepOutOfMemoryException();
        }
        try {
            return internalRegroup(regroupParams, rawRules);
        } finally {
            memory.releaseMemory(requiredMemory);
        }
    }

    private GroupLookup ensureGroupLookupCapacity(final RegroupParams regroupParams, final GroupRemapRule[] cleanRules)
        throws ImhotepOutOfMemoryException {
        int maxGroup = 0;

        for (final GroupRemapRule rule : cleanRules) {
            if (rule != null) {
                maxGroup = Math.max(maxGroup, Math.max(rule.negativeGroup, rule.positiveGroup));
            }
        }
        return namedGroupLookups.ensureWriteable(regroupParams, maxGroup);
    }

    private int internalRegroup(final RegroupParams regroupParams, final GroupRemapRule[] rawRules) throws ImhotepOutOfMemoryException {
        final int numGroups = namedGroupLookups.get(regroupParams.getInputGroups()).getNumGroups();
        final GroupRemapRule[] cleanRules = cleanUpRules(rawRules, numGroups);

        final GroupLookup docIdToGroup = ensureGroupLookupCapacity(regroupParams, cleanRules);
        final ThreadSafeBitSet docRemapped = new ThreadSafeBitSet(numDocs);
        try (final DocIdStream docIdStream = flamdexReader.getDocIdStream()) {
            applyIntConditions(docIdToGroup, cleanRules, docIdStream, docRemapped);
            applyStringConditions(docIdToGroup, cleanRules, docIdStream, docRemapped);
        }

        // pick up everything else that was missed
        for (int i = 0; i < docIdToGroup.size(); i++) {
            if (docRemapped.get(i)) {
                continue;
            }
            final int group = docIdToGroup.get(i);
            final int newGroup;
            if (cleanRules[group] != null) {
                newGroup = cleanRules[group].negativeGroup;
            } else {
                newGroup = 0;
            }
            docIdToGroup.set(i, newGroup);
        }

        return namedGroupLookups.finalizeRegroup(regroupParams);
    }

    @Override
    public int regroup(final RegroupParams regroupParams, final QueryRemapRule rule) throws ImhotepOutOfMemoryException {
        if (namedGroupLookups.handleFiltered(regroupParams)) {
            return 1;
        }

        final int maxOutputGroup = Math.max(rule.getNegativeGroup(), rule.getPositiveGroup());
        final GroupLookup docIdToGroup = namedGroupLookups.ensureWriteable(regroupParams, maxOutputGroup);

        final FastBitSetPooler bitSetPooler = new ImhotepBitSetPooler(memory);
        final FastBitSet bitSet;
        try {
            bitSet = bitSetPooler.create(flamdexReader.getNumDocs());
        } catch (final FlamdexOutOfMemoryException e) {
            throw newImhotepOutOfMemoryException(e);
        }

        try {
            final FlamdexSearcher searcher = new FlamdexSearcher(flamdexReader);
            final Query query = rule.getQuery();
            searcher.search(query, bitSet, bitSetPooler);
            docIdToGroup.bitSetRegroup(bitSet,
                                       rule.getTargetGroup(),
                                       rule.getNegativeGroup(),
                                       rule.getPositiveGroup());
        } catch (final FlamdexOutOfMemoryException e) {
            throw newImhotepOutOfMemoryException(e);
        } finally {
            bitSetPooler.release(bitSet.memoryUsage());
        }

        return namedGroupLookups.finalizeRegroup(regroupParams);
    }

    @Override
    public synchronized void intOrRegroup(final RegroupParams regroupParams,
                                          final String field,
                                          final long[] terms,
                                          final int targetGroup,
                                          final int negativeGroup,
                                          final int positiveGroup)
        throws ImhotepOutOfMemoryException {

        if (namedGroupLookups.handleFiltered(regroupParams)) {
            return;
        }

        final GroupLookup docIdToGroup = namedGroupLookups.ensureWriteable(regroupParams, Math.max(negativeGroup, positiveGroup));

        final FastBitSetPooler bitSetPooler = new ImhotepBitSetPooler(memory);
        final FastBitSet docRemapped;
        try {
            docRemapped = bitSetPooler.create(numDocs);
        } catch (final FlamdexOutOfMemoryException e) {
            throw newImhotepOutOfMemoryException(e);
        }

        try {
            try (
                final IntTermIterator iter = flamdexReader.getUnsortedIntTermIterator(field);
                final DocIdStream docIdStream = flamdexReader.getDocIdStream()
            ) {
                for (final long term : terms) {
                    iter.reset(term);
                    if (!iter.next() || iter.term() != term) {
                        continue;
                    }
                    docIdStream.reset(iter);
                    remapPositiveDocs(docIdToGroup, docIdStream, docRemapped, targetGroup, positiveGroup);
                }
            }
            remapNegativeDocs(docIdToGroup, docRemapped, targetGroup, negativeGroup);
        } finally {
            bitSetPooler.release(docRemapped.memoryUsage());
        }

        namedGroupLookups.finalizeRegroup(regroupParams);
    }

    @Override
    public synchronized void stringOrRegroup(final RegroupParams regroupParams,
                                             final String field,
                                             final String[] terms,
                                             final int targetGroup,
                                             final int negativeGroup,
                                             final int positiveGroup)
        throws ImhotepOutOfMemoryException {

        if (namedGroupLookups.handleFiltered(regroupParams)) {
            return;
        }

        final GroupLookup docIdToGroup = namedGroupLookups.ensureWriteable(regroupParams, Math.max(negativeGroup, positiveGroup));

        final FastBitSetPooler bitSetPooler = new ImhotepBitSetPooler(memory);
        final FastBitSet docRemapped;
        try {
            docRemapped = bitSetPooler.create(numDocs);
        } catch (final FlamdexOutOfMemoryException e) {
            throw newImhotepOutOfMemoryException(e);
        }
        try {
            try (
                final StringTermIterator iter = flamdexReader.getStringTermIterator(field);
                final DocIdStream docIdStream = flamdexReader.getDocIdStream()
            ) {
                for (final String term : terms) {
                    iter.reset(term);
                    if (!iter.next()) {
                        break;
                    }
                    if (!iter.term().equals(term)) {
                        continue;
                    }
                    docIdStream.reset(iter);
                    remapPositiveDocs(docIdToGroup, docIdStream, docRemapped, targetGroup, positiveGroup);
                }
            }
            remapNegativeDocs(docIdToGroup, docRemapped, targetGroup, negativeGroup);
        } finally {
            bitSetPooler.release(docRemapped.memoryUsage());
        }

        namedGroupLookups.finalizeRegroup(regroupParams);
    }

    @Override
    public void regexRegroup(final RegroupParams regroupParams,
                             final String field,
                             final String regex,
                             final int targetGroup,
                             final int negativeGroup,
                             final int positiveGroup)
        throws ImhotepOutOfMemoryException {
        if (getNumGroups() > 2) {
            throw newIllegalStateException("regexRegroup should be applied as a filter when you have only one group");
        }

        if (namedGroupLookups.handleFiltered(regroupParams)) {
            return;
        }

        final GroupLookup docIdToGroup = namedGroupLookups.ensureWriteable(regroupParams, Math.max(negativeGroup, positiveGroup));

        final FastBitSetPooler bitSetPooler = new ImhotepBitSetPooler(memory);
        final FastBitSet docRemapped;
        try {
            docRemapped = bitSetPooler.create(numDocs);
        } catch (final FlamdexOutOfMemoryException e) {
            throw newImhotepOutOfMemoryException(e);
        }
        try {
            try (
                final StringTermIterator iter = flamdexReader.getStringTermIterator(field);
                final DocIdStream docIdStream = flamdexReader.getDocIdStream()
            ) {
                final StringTermMatcher stringTermMatcher = StringTermMatchers.forRegex(regex);
                stringTermMatcher.run(iter, matchedIt -> {
                    docIdStream.reset(matchedIt);
                    remapPositiveDocs(docIdToGroup, docIdStream, docRemapped, targetGroup, positiveGroup);
                });
            }
            remapNegativeDocs(docIdToGroup, docRemapped, targetGroup, negativeGroup);
        } finally {
            bitSetPooler.release(docRemapped.memoryUsage());
        }

        namedGroupLookups.finalizeRegroup(regroupParams);
    }

    private void remapNegativeDocs(final GroupLookup docIdToGroup,
                                   final FastBitSet docRemapped,
                                   final int targetGroup,
                                   final int negativeGroup) {
        for (int doc = 0; doc < numDocs; ++doc) {
            if (!docRemapped.get(doc) && docIdToGroup.get(doc) == targetGroup) {
                docIdToGroup.set(doc, negativeGroup);
            }
        }
    }

    private void remapPositiveDocs(final GroupLookup docIdToGroup,
                                   final DocIdStream docIdStream,
                                   final FastBitSet docRemapped,
                                   final int targetGroup,
                                   final int positiveGroup) {
        // todo: refactor
        // replace remapPositiveDocs/remapNegativeDocs with
        // adding all docs to bitset and call GroupLookup.bitSetRegroup
        final int[] docIdBuf = memoryPool.getIntBuffer(BUFFER_SIZE, true);
        while (true) {
            final int n = docIdStream.fillDocIdBuffer(docIdBuf);
            for (int i = 0; i < n; ++i) {
                final int doc = docIdBuf[i];
                if (docIdToGroup.get(doc) == targetGroup) {
                    docIdToGroup.set(doc, positiveGroup);
                    docRemapped.set(doc);
                }
            }
            if (n < docIdBuf.length) {
                memoryPool.returnIntBuffer(docIdBuf);
                break;
            }
        }
    }

    private void remapDocs(final GroupLookup docIdToGroup,
                           final DocIdStream docIdStream,
                           final int from,
                           final int to) {
        final int[] docIdBuf = memoryPool.getIntBuffer(BUFFER_SIZE, true);
        while (true) {
            final int n = docIdStream.fillDocIdBuffer(docIdBuf);
            for (int i = 0; i < n; ++i) {
                final int doc = docIdBuf[i];
                if (docIdToGroup.get(doc) == from) {
                    docIdToGroup.set(doc, to);
                }
            }
            if (n < docIdBuf.length) {
                memoryPool.returnIntBuffer(docIdBuf);
                break;
            }
        }
    }

    @Override
    public synchronized void randomRegroup(final RegroupParams regroupParams,
                                           final String field,
                                           final boolean isIntField,
                                           final String salt,
                                           final double p,
                                           final int targetGroup,
                                           final int negativeGroup,
                                           final int positiveGroup)
        throws ImhotepOutOfMemoryException {
        if ((p < 0.0) || (p > 1.0)) {
            throw newIllegalArgumentException("p must be in range [0.0, 1.0]");
        }

        if (namedGroupLookups.handleFiltered(regroupParams)) {
            return;
        }

        final GroupLookup docIdToGroup = namedGroupLookups.ensureWriteable(regroupParams, Math.max(negativeGroup, positiveGroup));

        final FastBitSetPooler bitSetPooler = new ImhotepBitSetPooler(memory);
        final FastBitSet docRemapped;
        try {
            docRemapped = bitSetPooler.create(numDocs);
        } catch (final FlamdexOutOfMemoryException e) {
            throw newImhotepOutOfMemoryException(e);
        }
        try(final IterativeHasherUtils.TermHashIterator iterator =
                    IterativeHasherUtils.create(flamdexReader, field, isIntField, salt)) {
            final int threshold = IterativeHasherUtils.percentileToThreshold(p);
            final IterativeHasherUtils.GroupChooser chooser = new IterativeHasherUtils.TwoGroupChooser(threshold);
            while (iterator.hasNext()) {
                final int hash = iterator.getHash();
                if (chooser.getGroup(hash) == 1) {
                    final DocIdStream stream = iterator.getDocIdStream();
                    remapPositiveDocs(docIdToGroup, stream, docRemapped, targetGroup, positiveGroup);
                }
            }
            remapNegativeDocs(docIdToGroup, docRemapped, targetGroup, negativeGroup);
        } finally {
            bitSetPooler.release(docRemapped.memoryUsage());
        }

        namedGroupLookups.finalizeRegroup(regroupParams);
    }

    @Override
    public synchronized void randomMultiRegroup(final RegroupParams regroupParams,
                                                final String field,
                                                final boolean isIntField,
                                                final String salt,
                                                final int targetGroup,
                                                final double[] percentages,
                                                final int[] resultGroups)
        throws ImhotepOutOfMemoryException {
        ensureValidMultiRegroupArrays(percentages, resultGroups);

        if (namedGroupLookups.handleFiltered(regroupParams)) {
            return;
        }

        final GroupLookup docIdToGroup = namedGroupLookups.ensureWriteable(regroupParams, Ints.max(resultGroups));

        try(final IterativeHasherUtils.TermHashIterator iterator =
                    IterativeHasherUtils.create(flamdexReader, field, isIntField, salt)) {
            final IterativeHasherUtils.GroupChooser groupChooser =
                    IterativeHasherUtils.createChooser(percentages);
            while (iterator.hasNext()) {
                final int hash = iterator.getHash();
                final int groupIndex = groupChooser.getGroup(hash);
                final int newGroup = resultGroups[groupIndex];
                final DocIdStream stream = iterator.getDocIdStream();
                remapDocs(docIdToGroup, stream, targetGroup, newGroup);
            }
        }

        namedGroupLookups.finalizeRegroup(regroupParams);
    }

    @Override
    public synchronized void randomMetricRegroup(
            final RegroupParams regroupParams,
            final List<String> stat,
            final String salt,
            final double p,
            final int targetGroup,
            final int negativeGroup,
            final int positiveGroup
    ) throws ImhotepOutOfMemoryException {
        randomMetricMultiRegroup(
                regroupParams,
                stat,
                salt,
                targetGroup,
                new double[] {p},
                new int[] {negativeGroup, positiveGroup}
        );
    }

    @Override
    public synchronized void randomMetricMultiRegroup(
            final RegroupParams regroupParams,
            final List<String> stat,
            final String salt,
            final int targetGroup,
            final double[] percentages,
            final int[] resultGroups) throws ImhotepOutOfMemoryException {
        ensureValidMultiRegroupArrays(percentages, resultGroups);

        if (namedGroupLookups.handleFiltered(regroupParams)) {
            return;
        }

        try (final MetricStack stack = new MetricStack()) {
            final IntValueLookup lookup = stack.push(stat);
            final GroupLookup docIdToGroup = namedGroupLookups.ensureWriteable(regroupParams, Ints.max(resultGroups));

            // we want two ways of random regrouping to be equivalent
            // 1. session.pushStat(metric) + session.randomMetricMultiRegroup(metricIndex, ...)
            // 2. session.randomMultiRegroup(metric, ...)
            // That's why ConsistentLongHasher here.
            final IterativeHasher.ConsistentLongHasher hasher = new IterativeHasher.Murmur3Hasher(salt).consistentLongHasher();
            final IterativeHasherUtils.GroupChooser chooser = IterativeHasherUtils.createChooser(percentages);

            final int[] docIdBuf = memoryPool.getIntBuffer(BUFFER_SIZE, true);
            final long[] valBuf = memoryPool.getLongBuffer(BUFFER_SIZE, true);
            for (int startDoc = 0; startDoc < numDocs; startDoc += BUFFER_SIZE) {
                final int n = Math.min(BUFFER_SIZE, numDocs - startDoc);
                for (int i = 0; i < n; i++) {
                    docIdBuf[i] = startDoc + i;
                }
                lookup.lookup(docIdBuf, valBuf, n);
                for (int i = 0; i < n; i++) {
                    if (docIdToGroup.get(docIdBuf[i]) == targetGroup) {
                        final long val = valBuf[i];
                        final int hash = hasher.calculateHash(val);
                        final int groupIndex = chooser.getGroup(hash);
                        final int newGroup = resultGroups[groupIndex];
                        docIdToGroup.set(docIdBuf[i], newGroup);
                    }
                }
            }
            memoryPool.returnIntBuffer(docIdBuf);
            memoryPool.returnLongBuffer(valBuf);
        }
        namedGroupLookups.finalizeRegroup(regroupParams);
    }

    @Override
    public List<TermCount> approximateTopTerms(final String field,
                                               final boolean isIntField,
                                               int k) {
        k = Math.min(k, 1000);

        if (isIntField) {
            final PriorityQueue<IntTermWithFreq> pq =
                    new ObjectHeapPriorityQueue<>(k, INT_FREQ_COMPARATOR);
            try (IntTermIterator iter = flamdexReader.getUnsortedIntTermIterator(field)) {
                while (iter.next()) {
                    final int docFreq = iter.docFreq();
                    if (pq.size() < k) {
                        pq.enqueue(new IntTermWithFreq(iter.term(), docFreq));
                    } else {
                        final IntTermWithFreq min = pq.first();
                        if (docFreq > min.docFreq) {
                            min.term = iter.term();
                            min.docFreq = docFreq;
                            pq.changed();
                        }
                    }
                }
                final List<TermCount> ret = Lists.newArrayListWithCapacity(pq.size());
                while (!pq.isEmpty()) {
                    final IntTermWithFreq term = pq.dequeue();
                    ret.add(new TermCount(new Term(field, true, term.term, ""), term.docFreq));
                }
                Collections.reverse(ret);
                return ret;
            }
        } else {
            final PriorityQueue<StringTermWithFreq> pq =
                    new ObjectHeapPriorityQueue<>(k, STRING_FREQ_COMPARATOR);
            try (StringTermIterator iter = flamdexReader.getStringTermIterator(field)) {
                while (iter.next()) {
                    final int docFreq = iter.docFreq();
                    if (pq.size() < k) {
                        pq.enqueue(new StringTermWithFreq(iter.term(), docFreq));
                    } else {
                        final StringTermWithFreq min = pq.first();
                        if (docFreq > min.docFreq) {
                            min.term = iter.term();
                            min.docFreq = docFreq;
                            pq.changed();
                        }
                    }
                }
                final List<TermCount> ret = Lists.newArrayListWithCapacity(pq.size());
                while (!pq.isEmpty()) {
                    final StringTermWithFreq term = pq.dequeue();
                    ret.add(new TermCount(new Term(field, false, 0, term.term), term.docFreq));
                }
                Collections.reverse(ret);
                return ret;
            }
        }
    }

    // may use lhs destructively
    private static GroupLookup and(final GroupLookup lhs, final GroupLookup rhs) {
        Preconditions.checkArgument((lhs.getNumGroups() >= 1) && (lhs.getNumGroups() <= 2));
        Preconditions.checkArgument((rhs.getNumGroups() >= 1) && (rhs.getNumGroups() <= 2));
        if ((lhs instanceof ConstantGroupLookup) && (rhs instanceof ConstantGroupLookup)) {
            final boolean lhs1 = ((ConstantGroupLookup) lhs).getConstantGroup() == 1;
            final boolean rhs1 = ((ConstantGroupLookup) rhs).getConstantGroup() == 1;
            return new ConstantGroupLookup((lhs1 && rhs1) ? 1 : 0, lhs.size());
        } else if ((lhs instanceof BitSetGroupLookup) && (rhs instanceof BitSetGroupLookup)) {
            final BitSetGroupLookup lhs1 = (BitSetGroupLookup) lhs;
            final BitSetGroupLookup rhs1 = (BitSetGroupLookup) rhs;
            // this is where most work actually happens
            lhs1.and(rhs1);
            return lhs1;
        } else if ((lhs instanceof BitSetGroupLookup) && (rhs instanceof ConstantGroupLookup)) {
            final BitSetGroupLookup lhs1 = (BitSetGroupLookup) lhs;
            final boolean rhs1 = ((ConstantGroupLookup) rhs).getConstantGroup() == 1;
            if (!rhs1) {
                // (x && false) == false
                return rhs.makeCopy();
            }
            // (x && true) == x
            return lhs1;
        } else {
            Preconditions.checkState(lhs instanceof ConstantGroupLookup);
            final boolean lhs1 = ((ConstantGroupLookup) lhs).getConstantGroup() == 1;
            Preconditions.checkState(rhs instanceof BitSetGroupLookup);
            final BitSetGroupLookup rhs1 = (BitSetGroupLookup) rhs;

            if (lhs1) {
                // (true && x) == x
                return rhs1.makeCopy();
            } else {
                // (false && x) == false
                return lhs;
            }
        }
    }

    // may use lhs destructively
    private static GroupLookup or(final GroupLookup lhs, final GroupLookup rhs) {
        Preconditions.checkArgument((lhs.getNumGroups() >= 1) && (lhs.getNumGroups() <= 2));
        Preconditions.checkArgument((rhs.getNumGroups() >= 1) && (rhs.getNumGroups() <= 2));
        if ((lhs instanceof ConstantGroupLookup) && (rhs instanceof ConstantGroupLookup)) {
            final boolean lhs1 = ((ConstantGroupLookup) lhs).getConstantGroup() == 1;
            final boolean rhs1 = ((ConstantGroupLookup) rhs).getConstantGroup() == 1;
            return new ConstantGroupLookup((lhs1 || rhs1) ? 1 : 0, lhs.size());
        } else if ((lhs instanceof BitSetGroupLookup) && (rhs instanceof BitSetGroupLookup)) {
            final BitSetGroupLookup lhs1 = (BitSetGroupLookup) lhs;
            final BitSetGroupLookup rhs1 = (BitSetGroupLookup) rhs;
            // this is where most work actually happens
            lhs1.or(rhs1);
            return lhs1;
        } else if ((lhs instanceof BitSetGroupLookup) && (rhs instanceof ConstantGroupLookup)) {
            final BitSetGroupLookup lhs1 = (BitSetGroupLookup) lhs;
            final boolean rhs1 = ((ConstantGroupLookup) rhs).getConstantGroup() == 1;
            if (rhs1) {
                // (x || true) == true
                return rhs.makeCopy();
            }
            // (x || false) == x
            return lhs1;
        } else {
            Preconditions.checkState(lhs instanceof ConstantGroupLookup);
            final boolean lhs1 = ((ConstantGroupLookup) lhs).getConstantGroup() == 1;
            Preconditions.checkState(rhs instanceof BitSetGroupLookup);
            final BitSetGroupLookup rhs1 = (BitSetGroupLookup) rhs;

            if (lhs1) {
                // (true || x) == true
                return lhs;
            } else {
                // (false || x) == x
                return rhs1.makeCopy();
            }
        }
    }

    @Override
    public synchronized void consolidateGroups(List<String> inputGroups, final Operator operation, final String outputGroups) throws ImhotepOutOfMemoryException {
        // defensive copy
        inputGroups = Lists.newArrayList(inputGroups);

        final boolean allCorrectType = inputGroups.stream()
                .allMatch(x -> {
                    final GroupLookup value = namedGroupLookups.get(x);
                    return (value instanceof ConstantGroupLookup) || (value instanceof BitSetGroupLookup);
                });
        if (!allCorrectType) {
            throw new IllegalStateException("Can only use ConstantGroupLookup and BitSetGroupLookup with consolidateGroups");
        }

        final GroupLookup outputGroupLookup;
        switch (operation) {
            case AND:
            case OR:
                Preconditions.checkArgument(inputGroups.size() > 1, "AND/OR requires at least 2 arguments");
                GroupLookup computed;
                if (inputGroups.contains(outputGroups)) {
                    computed = namedGroupLookups.get(outputGroups);
                    inputGroups.remove(outputGroups);
                } else {
                    computed = namedGroupLookups.get(inputGroups.get(0)).makeCopy();
                    inputGroups.remove(0);
                }
                if (operation == Operator.AND) {
                    for (final String inputGroup : inputGroups) {
                        computed = and(computed, namedGroupLookups.get(inputGroup));
                    }
                } else {
                    for (final String inputGroup : inputGroups) {
                        computed = or(computed, namedGroupLookups.get(inputGroup));
                    }
                }
                outputGroupLookup = computed;
                break;

            case NOT:
                Preconditions.checkArgument(inputGroups.size() == 1, "NOT can only operate on one parameter");
                final String theInputGroups = inputGroups.get(0);
                final GroupLookup inputGroupLookup = namedGroupLookups.get(theInputGroups);
                Preconditions.checkArgument((inputGroupLookup.getNumGroups() >= 1) && (inputGroupLookup.getNumGroups() <= 2));
                if (inputGroupLookup instanceof BitSetGroupLookup) {
                    final BitSetGroupLookup outputBitSetGroupLookup;
                    if (theInputGroups.equals(outputGroups)) {
                        outputBitSetGroupLookup = (BitSetGroupLookup) inputGroupLookup;
                    } else {
                        outputBitSetGroupLookup = ((BitSetGroupLookup) inputGroupLookup).makeCopy();
                    }
                    outputBitSetGroupLookup.invertAllGroups();
                    outputGroupLookup = outputBitSetGroupLookup;
                } else {
                    Preconditions.checkState(inputGroupLookup instanceof ConstantGroupLookup);
                    final int constantGroup = ((ConstantGroupLookup) inputGroupLookup).getConstantGroup();
                    outputGroupLookup = new ConstantGroupLookup(1 - constantGroup, inputGroupLookup.size());
                }
                break;

            default:
                throw newIllegalArgumentException("Unknown operation: " + operation);
        }
        namedGroupLookups.claimMemoryAndPut(outputGroups, outputGroupLookup);
    }

    @Override
    public synchronized void deleteGroups(final List<String> groupsToDelete) {
        for (final String groupsName : groupsToDelete) {
            namedGroupLookups.delete(groupsName);
        }
    }

    private static final Comparator<IntTermWithFreq> INT_FREQ_COMPARATOR =
            new Comparator<IntTermWithFreq>() {
                @Override
                public int compare(final IntTermWithFreq o1, final IntTermWithFreq o2) {
                    return Ints.compare(o1.docFreq, o2.docFreq);
                }
            };

    private static final class IntTermWithFreq {
        public long term;
        public int docFreq;

        private IntTermWithFreq(final long term, final int docFreq) {
            this.term = term;
            this.docFreq = docFreq;
        }
    }

    private static final Comparator<StringTermWithFreq> STRING_FREQ_COMPARATOR =
            new Comparator<StringTermWithFreq>() {
                @Override
                public int compare(final StringTermWithFreq o1, final StringTermWithFreq o2) {
                    return Ints.compare(o1.docFreq, o2.docFreq);
                }
            };

    private static final class StringTermWithFreq {
        public String term;
        public int docFreq;

        private StringTermWithFreq(final String term, final int docFreq) {
            this.term = term;
            this.docFreq = docFreq;
        }
    }

    /**
     * Ensures that the percentages and resultGroups array are valid inputs for
     * a randomMultiRegroup. Otherwise, throws an IllegalArgumentException.
     * Specifically, checks to make sure
     * <ul>
     * <li>percentages is in ascending order,</li>
     * <li>percentages contains only values between 0.0 &amp; 1.0, and</li>
     * <li>len(percentages) == len(resultGroups) - 1</li>
     * </ul>
     *
     * @see ImhotepLocalSession#randomMultiRegroup(String, boolean, String, int,
     *      double[], int[])
     */
    protected void ensureValidMultiRegroupArrays(final double[] percentages,
                                                 final int[] resultGroups)
        throws IllegalArgumentException {
        // Ensure non-null inputs
        if (null == percentages || null == resultGroups) {
            throw newIllegalArgumentException("received null percentages or resultGroups to randomMultiRegroup");
        }

        // Ensure that the lengths are correct
        if (percentages.length != resultGroups.length - 1) {
            throw newIllegalArgumentException("percentages should have 1 fewer element than resultGroups");
        }

        ensurePercentagesValidity(percentages);
    }

    private void ensurePercentagesValidity(final double[] percentages) {
        // Ensure validity of percentages values
        double curr = 0.0;
        for (int i = 0; i < percentages.length; i++) {
            // Check: Increasing
            if (percentages[i] < curr) {
                throw newIllegalArgumentException("percentages values decreased between indices "
                        + (i - 1) + " and " + i);
            }

            // Check: between 0 and 1
            if (percentages[i] < 0.0 || percentages[i] > 1.0) {
                throw newIllegalArgumentException("percentages values should be between 0 and 1");
            }

            curr = percentages[i];
        }
    }

    @Override
    public synchronized int metricRegroup(final RegroupParams regroupParams,
                                          final List<String> stat,
                                          final long min,
                                          final long max,
                                          final long intervalSize,
                                          final boolean noGutters) throws ImhotepOutOfMemoryException {
        if (namedGroupLookups.handleFiltered(regroupParams)) {
            return 1;
        }

        final GroupLookup inputGroups = namedGroupLookups.get(regroupParams.getInputGroups());

        final int numBuckets = BigInteger
                .valueOf(max)
                .subtract(BigInteger.ONE)
                .subtract(BigInteger.valueOf(min))
                .divide(BigInteger.valueOf(intervalSize))
                .add(BigInteger.ONE)
                .intValueExact();
        final int totalBuckets = noGutters ? numBuckets : BigInteger.valueOf(numBuckets).add(BigInteger.valueOf(2)).intValueExact();
        final int newMaxGroup = BigInteger.valueOf(inputGroups.getNumGroups() - 1)
                .multiply(BigInteger.valueOf(totalBuckets))
                .intValueExact();

        if ((shardTimeRange != null) && Collections.singletonList("unixtime").equals(stat)) {
            final long minValueInclusive = shardTimeRange.getStartMillis() / 1000;
            final long maxValueExclusive = shardTimeRange.getEndMillis() / 1000;

            if ((min <= minValueInclusive) && (maxValueExclusive <= max)) {
                // check if all doc in shard go to one group
                // this often happens when grouping by unixtime (1h, 1d or 1mo for example)
                final int commonGroup = calculateCommonGroup(minValueInclusive, maxValueExclusive - 1, min, max, intervalSize, noGutters);
                if (commonGroup >= 0) {
                    if (commonGroup == 0) {
                        resetGroupsTo(regroupParams.getOutputGroups(), 0);
                    } else {
                        if (inputGroups instanceof ConstantGroupLookup) {
                            final int oldGroup = ((ConstantGroupLookup) inputGroups).getConstantGroup();
                            final int newGroup = ((oldGroup - 1) * totalBuckets) + commonGroup;
                            namedGroupLookups.claimMemoryAndPut(regroupParams.getOutputGroups(), new ConstantGroupLookup(newGroup, inputGroups.size()));
                        } else if (inputGroups instanceof BitSetGroupLookup) {
                            final BitSetGroupLookup bitSetGroupLookup = (BitSetGroupLookup) namedGroupLookups.copyInto(regroupParams);
                            final int oldGroup = bitSetGroupLookup.getNonZeroGroup();
                            final int newGroup = ((oldGroup - 1) * totalBuckets) + commonGroup;
                            bitSetGroupLookup.setNonZeroGroup(newGroup);
                        } else {
                            final GroupLookup outputGroups = namedGroupLookups.ensureWriteable(regroupParams, newMaxGroup);
                            // all in one interval
                            // TODO: rewrite on batched get/set after buffer pooling is introduced.
                            for (int i = 0; i < numDocs; i++) {
                                final int group = outputGroups.get(i);
                                if (group == 0) {
                                    continue;
                                }
                                outputGroups.set(i, (group - 1) * totalBuckets + commonGroup);
                            }
                        }
                    }
                    return namedGroupLookups.finalizeRegroup(regroupParams);
                }
            }
        }

        try (final MetricStack stack = new MetricStack()) {
            final IntValueLookup lookup = stack.push(stat);

            {
                // check if all doc in shard go to one group
                final int commonGroup = calculateCommonGroup(lookup.getMin(), lookup.getMax(), min, max, intervalSize, noGutters);
                if (commonGroup == 0) {
                    resetGroupsTo(regroupParams.getOutputGroups(), 0);
                    return namedGroupLookups.finalizeRegroup(regroupParams);
                } else if (commonGroup > 0) {
                    final GroupLookup docIdToGroup = namedGroupLookups.ensureWriteable(regroupParams, newMaxGroup);
                    // all in one interval
                    // TODO: rewrite on batched get/set after buffer pooling is introduced.
                    for (int i = 0; i < numDocs; i++) {
                        final int group = docIdToGroup.get(i);
                        if (group == 0) {
                            continue;
                        }
                        docIdToGroup.set(i, (group - 1) * totalBuckets + commonGroup);
                    }
                    return namedGroupLookups.finalizeRegroup(regroupParams);
                }
            }

            final GroupLookup docIdToGroup = namedGroupLookups.ensureWriteable(regroupParams, newMaxGroup);

            final int[] docIdBuf = memoryPool.getIntBuffer(BUFFER_SIZE, true);
            final int[] docGroupBuffer = memoryPool.getIntBuffer(BUFFER_SIZE, true);
            final long[] valBuf = memoryPool.getLongBuffer(BUFFER_SIZE, true);
            for (int doc = 0; doc < numDocs; doc += BUFFER_SIZE) {

                final int n = Math.min(BUFFER_SIZE, numDocs - doc);

                docIdToGroup.fillDocGrpBufferSequential(doc, docGroupBuffer, n);

                int numNonZero = 0;
                for (int i = 0; i < n; ++i) {
                    if (docGroupBuffer[i] != 0) {
                        docGroupBuffer[numNonZero] = docGroupBuffer[i];
                        docIdBuf[numNonZero++] = doc + i;
                    }
                }

                if (numNonZero == 0) {
                    continue;
                }

                lookup.lookup(docIdBuf, valBuf, numNonZero);

                if (noGutters) {
                    internalMetricRegroupNoGutters(min, max, intervalSize, numBuckets, numNonZero, valBuf, docGroupBuffer);
                } else {
                    internalMetricRegroupGutters(min, max, intervalSize, numBuckets, numNonZero, valBuf, docGroupBuffer);
                }

                docIdToGroup.batchSet(docIdBuf, docGroupBuffer, numNonZero);
            }

            memoryPool.returnIntBuffer(docIdBuf);
            memoryPool.returnIntBuffer(docGroupBuffer);
            memoryPool.returnLongBuffer(valBuf);
        }
        return namedGroupLookups.finalizeRegroup(regroupParams);
    }

    // calculate and return common group if exist or -1 if not exist
    private static int calculateCommonGroup(
            final long lookupMin,
            final long lookupMax,
            final long min,
            final long max,
            final long intervalSize,
            final boolean noGutters) {
        final int numBuckets = (int) (((max - 1) - min) / intervalSize + 1);
        if ((min <= lookupMin) && (lookupMax < max)) {
            final int minGroup = (int) ((lookupMin - min) / intervalSize + 1);
            final int maxGroup = (int) ((lookupMax - min) / intervalSize + 1);
            if (minGroup == maxGroup) {
                return minGroup;
            }
        } else if(lookupMax < min) {
            return noGutters ? 0 : (numBuckets + 1);
        } else if(max <= lookupMin) {
            return noGutters ? 0 : (numBuckets + 2);
        }

        return -1;
    }

    private void internalMetricRegroupGutters(final long min,
                                              final long max,
                                              final long intervalSize,
                                              final int numBuckets,
                                              final int numNonZero,
                                              final long[] valBuf,
                                              final int[] docGroupBuffer) {
        for (int i = 0; i < numNonZero; ++i) {
            final int group;
            final long val = valBuf[i];
            if (val < min) {
                group = numBuckets + 1;
            } else if (val >= max) {
                group = numBuckets + 2;
            } else {
                group = (int) ((val - min) / intervalSize + 1);
            }
            docGroupBuffer[i] = (docGroupBuffer[i] - 1) * (numBuckets + 2) + group;
        }
    }

    private void internalMetricRegroupNoGutters(final long min,
                                                final long max,
                                                final long intervalSize,
                                                final int numBuckets,
                                                final int numNonZero,
                                                final long[] valBuf,
                                                final int[] docGroupBuffer) {
        for (int i = 0; i < numNonZero; ++i) {
            final long val = valBuf[i];
            if (val < min || val >= max) {
                docGroupBuffer[i] = 0;
            } else {
                final int group = (int) ((val - min) / intervalSize + 1);
                docGroupBuffer[i] = (docGroupBuffer[i]-1)*numBuckets+group;
            }
        }
    }

    @Override
    public int regroup(
            final RegroupParams regroupParams,
            final int[] fromGroups,
            final int[] toGroups,
            final boolean filterOutNotTargeted) throws ImhotepOutOfMemoryException {
        if ((fromGroups == null) || (toGroups == null) || (fromGroups.length != toGroups.length)) {
            throw new IllegalArgumentException();
        }

        if (namedGroupLookups.handleFiltered(regroupParams)) {
            return 1;
        }

        final GroupLookup inputGroups = namedGroupLookups.get(regroupParams.getInputGroups());

        final int currentMaxGroup = inputGroups.getNumGroups() - 1;
        // maximum possible group after remapping
        int newMaxGroup = filterOutNotTargeted ? 0 : currentMaxGroup;
        for (int i = 0; i < fromGroups.length; i++) {
            if ((fromGroups[i] > 0) && (fromGroups[i] <= currentMaxGroup)) {
                newMaxGroup = Math.max(newMaxGroup, toGroups[i]);
            }
        }

        // form remap rules
        final int[] oldToNewGroup = new int[currentMaxGroup + 1];

        if (!filterOutNotTargeted) {
            for (int i = 0; i < oldToNewGroup.length; i++) {
                oldToNewGroup[i] = i;
            }
        }

        for (int i = 0; i < fromGroups.length; i++) {
            if (fromGroups[i] < oldToNewGroup.length) {
                oldToNewGroup[fromGroups[i]] = toGroups[i];
            }
        }

        // check for corner cases: everything is filtered out or nothing changed
        boolean hasChanges = false;
        for (int i = 0; i < oldToNewGroup.length; i++) {
            if (oldToNewGroup[i] != i) {
                hasChanges = true;
                break;
            }
        }

        if (!hasChanges) {
            final GroupLookup inputCopy = namedGroupLookups.get(regroupParams.getInputGroups()).makeCopy();
            namedGroupLookups.claimMemoryAndPut(regroupParams.getOutputGroups(), inputCopy);
            return namedGroupLookups.finalizeRegroup(regroupParams);
        }

        boolean filteredOut = true;
        for (final int group : oldToNewGroup) {
            if (group != 0) {
                filteredOut = false;
                break;
            }
        }

        if (filteredOut) {
            resetGroupsTo(regroupParams.getOutputGroups(), 0);
            return namedGroupLookups.finalizeRegroup(regroupParams);
        }

        // do a remap
        final GroupLookup docIdToGroup = namedGroupLookups.ensureWriteable(regroupParams, newMaxGroup);

        // TODO: Batch method for setting all values?
        for (int docId = 0; docId < numDocs; docId++) {
            final int oldGroup = docIdToGroup.get(docId);
            final int newGroup = oldToNewGroup[oldGroup];
            docIdToGroup.set(docId, newGroup);
        }

        return namedGroupLookups.finalizeRegroup(regroupParams);
    }

    public synchronized int metricFilter(final RegroupParams regroupParams, final List<String> stat, final long min, final long max, final boolean negate) throws ImhotepOutOfMemoryException {
        if (namedGroupLookups.handleFiltered(regroupParams)) {
            return 1;
        }

        try (MetricStack stack = new MetricStack()) {
            final IntValueLookup lookup = stack.push(stat);

            final GroupLookup docIdToGroup = namedGroupLookups.ensureWriteable(regroupParams);

            final int numDocs = docIdToGroup.size();
            final int[] docIdBuf = memoryPool.getIntBuffer(BUFFER_SIZE, true);
            final int[] docGroupBuffer = memoryPool.getIntBuffer(BUFFER_SIZE, true);
            final long[] valBuf = memoryPool.getLongBuffer(BUFFER_SIZE, true);
            for (int doc = 0; doc < numDocs; doc += BUFFER_SIZE) {

                final int n = Math.min(BUFFER_SIZE, numDocs - doc);

                docIdToGroup.fillDocGrpBufferSequential(doc, docGroupBuffer, n);

                int numNonZero = 0;
                for (int i = 0; i < n; ++i) {
                    final int group = docGroupBuffer[i];
                    if (group != 0) {
                        docIdBuf[numNonZero] = doc + i;
                        docGroupBuffer[numNonZero++] = group;
                    }
                }

                if (numNonZero == 0) {
                    continue;
                }

                lookup.lookup(docIdBuf, valBuf, numNonZero);

                for (int i = 0; i < numNonZero; ++i) {
                    final long val = valBuf[i];
                    final boolean valInRange = val >= min && val <= max;
                    if (valInRange == negate) {
                        docGroupBuffer[i] = 0;
                    }
                }

                docIdToGroup.batchSet(docIdBuf, docGroupBuffer, numNonZero);
            }

            memoryPool.returnIntBuffer(docIdBuf);
            memoryPool.returnIntBuffer(docGroupBuffer);
            memoryPool.returnLongBuffer(valBuf);
        }

        return namedGroupLookups.finalizeRegroup(regroupParams);
    }

    @Override
    public synchronized int metricFilter(final RegroupParams regroupParams, final List<String> stat, final long min, final long max, final int targetGroup, final int negativeGroup, final int positiveGroup) throws ImhotepOutOfMemoryException {
        final GroupLookup inputGroups = namedGroupLookups.get(regroupParams.getInputGroups());
        if (namedGroupLookups.handleFiltered(regroupParams) || (targetGroup >= inputGroups.getNumGroups())) {
            return inputGroups.getNumGroups();
        }

        try (MetricStack stack = new MetricStack()) {
            final IntValueLookup lookup = stack.push(stat);

            final int newMaxGroup = Math.max(inputGroups.getNumGroups(), Math.max(positiveGroup, negativeGroup));
            final GroupLookup docIdToGroup = namedGroupLookups.ensureWriteable(regroupParams, newMaxGroup);

            final int numDocs = docIdToGroup.size();
            final int[] docIdBuf = memoryPool.getIntBuffer(BUFFER_SIZE, true);
            final int[] docGroupBuffer = memoryPool.getIntBuffer(BUFFER_SIZE, true);
            final long[] valBuf = memoryPool.getLongBuffer(BUFFER_SIZE, true);
            for (int doc = 0; doc < numDocs; doc += BUFFER_SIZE) {

                final int n = Math.min(BUFFER_SIZE, numDocs - doc);

                docIdToGroup.fillDocGrpBufferSequential(doc, docGroupBuffer, n);

                int numTargeted = 0;
                for (int i = 0; i < n; ++i) {
                    final int group = docGroupBuffer[i];
                    if (group == targetGroup) {
                        docIdBuf[numTargeted] = doc + i;
                        docGroupBuffer[numTargeted++] = group;
                    }
                }

                if (numTargeted == 0) {
                    continue;
                }

                lookup.lookup(docIdBuf, valBuf, numTargeted);

                for (int i = 0; i < numTargeted; ++i) {
                    final long val = valBuf[i];
                    final boolean valInRange = val >= min && val <= max;
                    docGroupBuffer[i] = valInRange ? positiveGroup : negativeGroup;
                }

                docIdToGroup.batchSet(docIdBuf, docGroupBuffer, numTargeted);
            }

            memoryPool.returnIntBuffer(docIdBuf);
            memoryPool.returnIntBuffer(docGroupBuffer);
            memoryPool.returnLongBuffer(valBuf);
        }

        return namedGroupLookups.finalizeRegroup(regroupParams);
    }

    private static GroupRemapRule[] cleanUpRules(final GroupRemapRule[] rawRules, final int numGroups) {
        final GroupRemapRule[] cleanRules = new GroupRemapRule[numGroups];
        for (final GroupRemapRule rawRule : rawRules) {
            if (rawRule.targetGroup >= cleanRules.length) {
                continue; // or error?
            }
            if (cleanRules[rawRule.targetGroup] != null) {
                continue; // or error?
            }
            cleanRules[rawRule.targetGroup] = rawRule;
        }
        return cleanRules;
    }

    private static final String decimalPattern = "-?[0-9]*\\.?[0-9]+";

    private static final Pattern floatScalePattern =
            Pattern.compile("floatscale\\s+(\\w+)\\s*\\*\\s*(" + decimalPattern + ")\\s*\\+\\s*("
                    + decimalPattern + ")");

    private static final Pattern REGEXPMATCH_COMMAND = Pattern.compile("regexmatch\\s+(\\w+)\\s+([0-9]+)\\s(.+)");
    private static final Pattern RANDOM_PATTERN = Pattern.compile("^random\\s+(?<type>int|str)\\s+\\[(?<percentiles>[0-9., ]+)]\\s+(?<field>.*)\\s+\"(?<salt>.*)\"$");
    private static final Pattern RANDOM_METRIC_PATTERN = Pattern.compile("^random_metric\\s+\\[(?<percentiles>[0-9., ]+)]\\s+\"(?<salt>.*)\"$");

    class MetricStack implements Closeable {
        private int numStats;
        private final StatLookup stats = new StatLookup(MAX_NUMBER_STATS);
        private final List<String> statCommands = new ArrayList<>();

        public int getNumStats() {
            return numStats;
        }

        public void push(final String name, final IntValueLookup lookup) {
            if (numStats == MAX_NUMBER_STATS) {
                throw newIllegalArgumentException("Maximum number of stats exceeded");
            }

            stats.set(numStats, name, lookup);
            numStats += 1;
        }

        public IntValueLookup popLookup() {
            if (numStats == 0) {
                throw newIllegalStateException("no stat to pop");
            }
            --numStats;

            final IntValueLookup ret = stats.get(numStats);
            stats.set(numStats, null, null);

            return ret;
        }

        public void addObserver(final StatLookup.Observer observer) {
            stats.addObserver(observer);
        }

        /**
         * The resulting value will still be present in the metric stack and should NOT be closed by the caller in
         * any circumstance.
         */
        public IntValueLookup get(final int index) {
            if ((index < 0) || (index >= numStats)) {
                throw newIllegalArgumentException("invalid stat index: " + index
                        + ", must be between [0," + numStats + ")");
            }
            IntValueLookup result = stats.get(index);
            // TODO: Is this dumb since we have a fancy JIT?
            // Try to avoid unnecessary double dispatch.
            while (result instanceof DelegatingMetric) {
                result = ((DelegatingMetric) result).getInner();
            }
            return result;
        }

        public IntValueLookup set(final int index, final String statName, final IntValueLookup lookup) {
            return stats.set(index, statName, lookup);
        }

        public List<String> getStatCommands() {
            return Collections.unmodifiableList(statCommands);
        }

        public void clearStatCommands() {
            Preconditions.checkState(numStats == 0, "Cannot clear stat commands if numStats!=0");
            statCommands.clear();
        }

        @Override
        public void close() {
            stats.close();
        }

        public IntValueLookup push(final List<String> stat) throws ImhotepOutOfMemoryException {
            final int startNumStats = numStats;
            for (final String s : stat) {
                pushStat(s, this);
            }
            Preconditions.checkState(numStats == (startNumStats + 1), "push(List<String>) should increase stat depth by exactly 1");
            return get(numStats - 1);
        }
    }

    /**
     * If stats is non-null, then push the stats present there.
     * If stats is null, then push delegating stats from ImhotepLocalSession.this.metricStack
     */
    public MetricStack fromStatsOrStackCopy(@Nullable final List<List<String>> stats) throws ImhotepOutOfMemoryException {
        final MetricStack stack = new MetricStack();
        try {
            if (stats != null) {
                for (final List<String> stat : stats) {
                    stack.push(stat);
                }
            } else {
                for (int i = 0; i < metricStack.getNumStats(); i++) {
                    pushStat("global_stack " + i, stack);
                }
            }
            return stack;
        } catch (final Exception e) {
            Closeables2.closeQuietly(stack, log);
            throw e;
        }
    }

    @Override
    public synchronized int pushStat(final String statName) throws ImhotepOutOfMemoryException {
        return pushStat(statName, metricStack);
    }

    public int pushStat(String statName, final MetricStack stack) throws ImhotepOutOfMemoryException {
        if (statName.startsWith("hasstr ")) {
            final String s = statName.substring("hasstr ".length()).trim();
            final String[] split = s.split(":", 2);
            if (split.length < 2) {
                throw newIllegalArgumentException("invalid hasstr metric: " + statName);
            }
            stack.push(statName, hasStringTermFilter(split[0], split[1]));
        } else if (statName.startsWith("hasint ")) {
            final String s = statName.substring("hasint ".length()).trim();
            final String[] split = s.split(":", 2);
            if (split.length < 2) {
                throw newIllegalArgumentException("invalid hasint metric: " + statName);
            }
            stack.push(statName, hasIntTermFilter(split[0], Long.parseLong(split[1])));
        } else if (statName.startsWith("hasstrfield ")) {
            final String field = statName.substring("hasstrfield ".length()).trim();
            stack.push(statName, hasStringFieldFilter(field));
        } else if (statName.startsWith("hasintfield ")) {
            final String field = statName.substring("hasintfield ".length()).trim();
            stack.push(statName, hasIntFieldFilter(field));
        } else if (statName.startsWith("regex ")) {
            final String s = statName.substring("regex ".length()).trim();
            final String[] split = s.split(":", 2);
            if (split.length < 2) {
                throw newIllegalArgumentException("invalid regex metric: " + statName);
            }
            stack.push(statName, hasRegexFilter(split[0], split[1]));
        } else if (statName.startsWith("fieldequal ")) {
            final String s = statName.substring("fieldequal ".length()).trim();
            final String[] split = s.split("=");
            if (split.length != 2) {
                throw newIllegalArgumentException("invalid field equal: " + statName);
            }
            stack.push(statName, fieldEqualFilter(split[0], split[1]));
        } else if (statName.startsWith("regexmatch ")) {
            final Matcher matcher = REGEXPMATCH_COMMAND.matcher(statName);
            if (!matcher.matches()) {
                throw newIllegalArgumentException("invalid regexmatch metric: " + statName);
            }
            final String fieldName = matcher.group(1);
            final int matchIndex = Integer.parseInt(matcher.group(2));
            final String regexp = matcher.group(3);

            if (matchIndex < 1) {
                throw newIllegalArgumentException("invalid regexmatch index: " + statName);
            }

            stack.push(statName, matchByRegex(fieldName, regexp, matchIndex));
        } else if (statName.startsWith("inttermcount ")) {
            final String field = statName.substring("inttermcount ".length()).trim();
            stack.push(statName, termCountLookup(field, true));
        } else if (statName.startsWith("strtermcount ")) {
            final String field = statName.substring("strtermcount ".length()).trim();
            stack.push(statName, termCountLookup(field, false));
        } else if (statName.startsWith("floatscale ")) {
            final Matcher matcher = floatScalePattern.matcher(statName);
            // accepted format is 'floatscale field*scale+offset' (or just look
            // at the pattern)
            if (!matcher.matches()) {
                throw newIllegalArgumentException("invalid floatscale metric: " + statName);
            }

            final String field = matcher.group(1);
            final double scale;
            final double offset;
            try {
                scale = Double.parseDouble(matcher.group(2));
                offset = Double.parseDouble(matcher.group(3));
            } catch (final NumberFormatException e) {
                throw newIllegalArgumentException("invalid offset or scale constant for metric: "
                        + statName,  e);
            }

            stack.push(statName, scaledFloatLookup(field, scale, offset));
        } else if (statName.startsWith("dynamic ")) {
            final String name = statName.substring("dynamic ".length()).trim();
            final DynamicMetric metric = getDynamicMetrics().get(name);
            if (metric == null) {
                throw newIllegalArgumentException("invalid dynamic metric: " + name);
            }
            stack.push(statName, metric);
        } else if (statName.startsWith("exp ")) {
            final int scaleFactor = Integer.valueOf(statName.substring("exp ".length()).trim());
            final IntValueLookup operand = stack.popLookup();
            stack.push(statName, new Exponential(operand, scaleFactor));
        } else if (statName.startsWith("log ")) {
            final int scaleFactor = Integer.valueOf(statName.substring("log ".length()).trim());
            final IntValueLookup operand = stack.popLookup();
            stack.push(statName, new Log(operand, scaleFactor));
        } else if (statName.startsWith("ref ")) {
            final int depth = Integer.valueOf(statName.substring("ref ".length()).trim());
            stack.push(statName, new DelegatingMetric(stack.get(stack.getNumStats() - depth - 1)));
        } else if (statName.startsWith("len ")) {
            final String field = statName.substring("len ".length()).trim();
            stack.push(statName, stringLenLookup(field));
        } else if (is32BitInteger(statName)) {
            final int constant = Integer.parseInt(statName); // guaranteed not to fail
            stack.push(statName, new Constant(constant));
        } else if (is64BitInteger(statName)) {
            final long constant = Long.parseLong(statName); // guaranteed notto fail
            stack.push(statName, new Constant(constant));
        } else if (statName.startsWith("interleave ")) {
            final int count = Integer.valueOf(statName.substring("interleave ".length()).trim());

            final IntValueLookup[] originals = new IntValueLookup[count];
            final int start = stack.getNumStats() - count;
            if (start < 0) {
                throw newIllegalArgumentException(statName + ": expected at least " + count
                        + " metrics on stack, found " + stack.getNumStats());
            }

            for (int i = 0; i < count; i++) {
                originals[i] = stack.get(start + i);
            }
            final IntValueLookup[] cached =
                    new CachedInterleavedMetrics(memory, flamdexReader.getNumDocs(), originals).getLookups();

            for (int i = 0; i < count; i++) {
                final IntValueLookup oldValue = stack.set(start + i, statName, cached[i]);
                Preconditions.checkNotNull(oldValue, "interleave should never set previously unset metric indexes");
                oldValue.close();
            }
        } else if (statName.startsWith("mulshr ")) {
            final int shift = Integer.valueOf(statName.substring("mulshr ".length()).trim());
            if (shift < 0 || shift > 31) {
                throw newIllegalArgumentException("mulshr shift value must be between 0 and 31 (inclusive)");
            }
            final IntValueLookup b = stack.popLookup();
            final IntValueLookup a = stack.popLookup();
            stack.push(statName, new ShiftRight(new Multiplication(a, b), shift));
        } else if (statName.startsWith("shldiv ")) {
            final int shift = Integer.valueOf(statName.substring("shldiv ".length()).trim());
            if (shift < 0 || shift > 31) {
                throw newIllegalArgumentException("shldiv shift value must be between 0 and 31 (inclusive)");
            }
            final IntValueLookup b = stack.popLookup();
            final IntValueLookup a = stack.popLookup();
            stack.push(statName, new Division(new ShiftLeft(a, shift), b));
        } else if (statName.startsWith("log1pexp ")) {
            final int scale = Integer.valueOf(statName.substring("log1pexp ".length()).trim());
            final IntValueLookup operand = stack.popLookup();
            stack.push(statName, new Log1pExp(operand, scale));
        } else if (statName.startsWith("logistic ")) {
            final String[] params = statName.substring("logistic ".length()).split(" ");
            if (params.length != 2) {
                throw newIllegalArgumentException("logistic requires 2 arguments: "+statName);
            }
            final double scaleDown;
            final double scaleUp;
            try {
                scaleDown = Double.parseDouble(params[0]);
                scaleUp = Double.parseDouble(params[1]);
            } catch (final NumberFormatException e) {
                throw newIllegalArgumentException("invalid scale factor for metric: "
                        + statName,  e);
            }
            final IntValueLookup operand = stack.popLookup();
            stack.push(statName, new Logistic(operand, scaleDown, scaleUp));
        } else if (statName.startsWith("lucene ")) {
            final String queryBase64 = statName.substring("lucene ".length());
            final byte[] queryBytes = Base64.decodeBase64(queryBase64.getBytes());
            final QueryMessage queryMessage;
            try {
                queryMessage = QueryMessage.parseFrom(queryBytes);
            } catch (final InvalidProtocolBufferException e) {
                throw newRuntimeException(e);
            }
            final Query query = ImhotepDaemonMarshaller.marshal(queryMessage);

            final int bitSetMemory = (flamdexReader.getNumDocs() + 64) / 64 * 8;
            if (!memory.claimMemory(bitSetMemory)) {
                throw newImhotepOutOfMemoryException();
            }
            try {
                final FastBitSet bitSet = new FastBitSet(flamdexReader.getNumDocs());
                final FastBitSetPooler bitSetPooler = new ImhotepBitSetPooler(memory);
                final FlamdexSearcher searcher = new FlamdexSearcher(flamdexReader);
                searcher.search(query, bitSet, bitSetPooler);
                final IntValueLookup lookup = new MemoryReservingIntValueLookupWrapper(
                        new com.indeed.flamdex.fieldcache.BitSetIntValueLookup(bitSet));
                stack.push(statName, lookup);
            } catch (final Throwable t) {
                memory.releaseMemory(bitSetMemory);
                if (t instanceof FlamdexOutOfMemoryException) {
                    throw newImhotepOutOfMemoryException(t);
                }
                throw Throwables2.propagate(t, ImhotepOutOfMemoryException.class);
            }
        } else if (statName.startsWith("random ")) {
            // Expected result:
            //      0 if document does not have the field.
            //      1 through (percentiles.length + 1), where the distribution across these groups is determined
            //        by the values in percentiles, and for any individual document is determined by the
            //        hash of the term+salt
            final Matcher matcher = RANDOM_PATTERN.matcher(statName);
            if (!matcher.matches()) {
                throw new IllegalArgumentException("random stat \"" + statName + "\" does not match the pattern: " + matcher.pattern());
            }

            final boolean isIntField = "int".equals(matcher.group("type"));

            final double[] percentiles = Arrays.stream(matcher.group("percentiles").split(","))
                    .mapToDouble(Double::parseDouble)
                    .toArray();

            final String salt = matcher.group("salt");
            final String field = matcher.group("field").trim();

            stack.push(statName, randomLookup(field, isIntField, salt, percentiles));
        } else if (statName.startsWith("random_metric ")) {
            // Expected result:
            //      1 through (percentiles.length + 1), where the distribution across these groups is determined
            //        by the values in percentiles, and for any individual document is determined by the
            //        hash of the popped metric+salt
            final Matcher matcher = RANDOM_METRIC_PATTERN.matcher(statName);
            if (!matcher.matches()) {
                throw new IllegalArgumentException("random stat \"" + statName + "\" does not match the pattern: " + matcher.pattern());
            }

            final double[] percentiles = Arrays.stream(matcher.group("percentiles").split(","))
                    .mapToDouble(Double::parseDouble)
                    .toArray();

            final String salt = matcher.group("salt");

            try (final IntValueLookup operand = stack.popLookup()) {
                stack.push(statName, randomMetricLookup(operand, salt, percentiles));
            }
        } else if (statName.startsWith("global_stack ")) {
            final int statIndex = Integer.parseInt(statName.substring("global_stack ".length()));
            stack.push(statName, new DelegatingMetric(metricStack.get(statIndex)));
        } else if (Metric.getMetric(statName) != null) {
            final IntValueLookup a;
            final IntValueLookup b;
            switch (Metric.getMetric(statName)) {
            case COUNT:
                stack.push(statName, new Count());
                break;
            case CACHED:
                a = stack.popLookup();
                try {
                    stack.push(statName, new CachedMetric(a, flamdexReader.getNumDocs(), memory));
                } finally {
                    a.close();
                }
                break;
            case ADD:
                b = stack.popLookup();
                a = stack.popLookup();
                stack.push(statName, new Addition(a, b));
                break;
            case SUBTRACT:
                b = stack.popLookup();
                a = stack.popLookup();
                stack.push(statName, new Subtraction(a, b));
                break;
            case MULTIPLY:
                b = stack.popLookup();
                a = stack.popLookup();
                stack.push(statName, new Multiplication(a, b));
                break;
            case DIVIDE:
                b = stack.popLookup();
                a = stack.popLookup();
                stack.push(statName, new Division(a, b));
                break;
            case MODULUS:
                b = stack.popLookup();
                a = stack.popLookup();
                stack.push(statName, new Modulus(a, b));
                break;
            case ABSOLUTE_VALUE:
                a = stack.popLookup();
                stack.push(statName, new AbsoluteValue(a));
                break;
            case MIN:
                b = stack.popLookup();
                a = stack.popLookup();
                stack.push(statName, new Min(a, b));
                break;
            case MAX:
                b = stack.popLookup();
                a = stack.popLookup();
                stack.push(statName, new Max(a, b));
                break;
            case EQ:
                b = stack.popLookup();
                a = stack.popLookup();
                stack.push(statName, new Equal(a, b));
                break;
            case NE:
                b = stack.popLookup();
                a = stack.popLookup();
                stack.push(statName, new NotEqual(a, b));
                break;
            case LT:
                b = stack.popLookup();
                a = stack.popLookup();
                stack.push(statName, new LessThan(a, b));
                break;
            case LTE:
                b = stack.popLookup();
                a = stack.popLookup();
                stack.push(statName, new LessThanOrEqual(a, b));
                break;
            case GT:
                b = stack.popLookup();
                a = stack.popLookup();
                stack.push(statName, new GreaterThan(a, b));
                break;
            case GTE:
                b = stack.popLookup();
                a = stack.popLookup();
                stack.push(statName, new GreaterThanOrEqual(a, b));
                break;
            case DOCID:
                stack.push(statName, new DocIdMetric(numDocs));
                break;
            default:
                throw newRuntimeException("this is a bug");
            }
        } else {
            try {
                // Temporary hack to allow transition from Lucene to Flamdex
                // shards where the time metric has a different name
                if("time".equals(statName) && flamdexReader.getIntFields().contains("unixtime")) {
                    statName = "unixtime";
                } else if(statName.equals("unixtime") && !flamdexReader.getIntFields().contains("unixtime")) {
                    statName = "time";
                }

                stack.push(statName, flamdexReader.getMetric(statName));
            } catch (final FlamdexOutOfMemoryException e) {
                throw newImhotepOutOfMemoryException(e);
            }
        }

        /* this request is valid, so keep track of the command */
        stack.statCommands.add(statName);

        return stack.numStats;
    }

    @Override
    public synchronized int pushStats(final List<String> statNames)
        throws ImhotepOutOfMemoryException {
        for (final String statName : statNames) {
            this.pushStat(statName);
        }

        return metricStack.getNumStats();
    }

    private static boolean is32BitInteger(final String s) {
        try {
            Integer.parseInt(s);
            return true;
        } catch (final NumberFormatException e) {
            return false;
        }
    }

    private static boolean is64BitInteger(final String s) {
        try {
            Long.parseLong(s);
            return true;
        } catch (final NumberFormatException e) {
            return false;
        }
    }

    @Override
    public synchronized int popStat() {
        metricStack.popLookup().close();
        metricStack.statCommands.add("pop");
        return metricStack.getNumStats();
    }

    @Override
    public synchronized int getNumStats() {
        return metricStack.getNumStats();
    }

    @Override
    public int getNumGroups(final String groupsName) {
        return Objects.requireNonNull(namedGroupLookups.get(groupsName), "Groups named " + groupsName + " do not exist")
                .getNumGroups();
    }

    @Override
    public synchronized void createDynamicMetric(final String name)
        throws ImhotepOutOfMemoryException {
        if (getDynamicMetrics().containsKey(name)) {
            throw newRuntimeException("dynamic metric \"" + name + "\" already exists");
        }
        if (!memory.claimMemory(flamdexReader.getNumDocs() * 4L)) {
            throw newImhotepOutOfMemoryException();
        }
        getDynamicMetrics().put(name, new DynamicMetric(flamdexReader.getNumDocs()));
    }

    @Override
    public synchronized void updateDynamicMetric(final String groupsName, final String name, final int[] deltas)
        throws ImhotepOutOfMemoryException {
        final DynamicMetric metric = getDynamicMetrics().get(name);
        if (metric == null) {
            throw newRuntimeException("dynamic metric \"" + name + "\" does not exist");
        }

        final GroupLookup docIdToGroup = namedGroupLookups.get(groupsName);

        final int numDocs = flamdexReader.getNumDocs();
        final DynamicMetric.Editor editor = metric.getEditor();
        for (int doc = 0; doc < numDocs; doc++) {
            final int group = docIdToGroup.get(doc);
            if (group >= 0 && group < deltas.length) {
                editor.add(doc, deltas[group]);
            }
        }
    }

    @Override
    public synchronized void conditionalUpdateDynamicMetric(final String name,
                                                            final RegroupCondition[] conditions,
                                                            final int[] deltas) {
        validateConditionalUpdateDynamicMetricInput(conditions, deltas);
        final DynamicMetric metric = getDynamicMetrics().get(name);
        if (metric == null) {
            throw newRuntimeException("dynamic metric \"" + name + "\" does not exist");
        }

        final List<Integer> indexes = Lists.newArrayList();
        for (int i = 0; i < conditions.length; i++) {
            indexes.add(i);
        }

        // I don't think it's really worth claiming memory for this, so I won't.
        final ImmutableListMultimap<Pair<String, Boolean>, Integer> fieldIndexMap =
                Multimaps.index(indexes, new Function<Integer, Pair<String, Boolean>>() {
                    @Override
                    public Pair<String, Boolean> apply(final Integer index) {
                        return Pair.of(conditions[index].field, conditions[index].intType);
                    }
                });
        for (final Pair<String, Boolean> field : fieldIndexMap.keySet()) {
            final String fieldName = field.getFirst();
            final boolean fieldIsIntType = field.getSecond();
            final List<Integer> indices = Lists.newArrayList(fieldIndexMap.get(field));
            // Sort within the field
            Collections.sort(indices, new Comparator<Integer>() {
                @Override
                public int compare(final Integer o1, final Integer o2) {
                    if (fieldIsIntType) {
                        return Longs.compare(conditions[o1].intTerm, conditions[o2].intTerm);
                    } else {
                        return conditions[o1].stringTerm.compareTo(conditions[o2].stringTerm);
                    }
                }
            });
            final DocIdStream docIdStream = flamdexReader.getDocIdStream();
            final int[] docIdBuf = memoryPool.getIntBuffer(BUFFER_SIZE, true);
            if (fieldIsIntType) {
                final IntTermIterator termIterator = flamdexReader.getUnsortedIntTermIterator(fieldName);
                for (final int index : indices) {
                    final long term = conditions[index].intTerm;
                    final int delta = deltas[index];
                    termIterator.reset(term);
                    if (termIterator.next() && termIterator.term() == term) {
                        docIdStream.reset(termIterator);
                        adjustDeltas(metric, docIdStream, delta, docIdBuf);
                    }
                }
            } else {
                final StringTermIterator termIterator =
                        flamdexReader.getStringTermIterator(fieldName);
                for (final int index : indices) {
                    final String term = conditions[index].stringTerm;
                    final int delta = deltas[index];
                    termIterator.reset(term);
                    if (termIterator.next() && termIterator.term().equals(term)) {
                        docIdStream.reset(termIterator);
                        adjustDeltas(metric, docIdStream, delta, docIdBuf);
                    }
                }
            }
            memoryPool.returnIntBuffer(docIdBuf);
        }
    }

    private void validateConditionalUpdateDynamicMetricInput(final RegroupCondition[] conditions,
                                                             final int[] deltas) {
        if (conditions.length != deltas.length) {
            throw newIllegalArgumentException("conditions and deltas must be of the same length");
        }
        for (final RegroupCondition condition : conditions) {
            if (condition.inequality) {
                throw newIllegalArgumentException(
                        "inequality conditions not currently supported by conditionalUpdateDynamicMetric!");
            }
        }
    }

    private void validateQueryUpdateDynamicMetricInput(
            final Query[] conditions,
            final int[] deltas) {
        if (conditions.length != deltas.length) {
            throw newIllegalArgumentException("conditions and deltas must be of the same length");
        }
    }

    public void groupConditionalUpdateDynamicMetric(
            final String groupsName,
            final String name,
            final int[] groups,
            final RegroupCondition[] conditions,
            final int[] deltas) {
        if (groups.length != conditions.length) {
            throw newIllegalArgumentException("groups and conditions must be the same length");
        }
        validateConditionalUpdateDynamicMetricInput(conditions, deltas);
        final DynamicMetric metric = getDynamicMetrics().get(name);
        if (metric == null) {
            throw newRuntimeException("dynamic metric \"" + name + "\" does not exist");
        }
        final IntArrayList groupsSet = new IntArrayList();

        final GroupLookup docIdToGroup = namedGroupLookups.get(groupsName);
        final int numGroups = docIdToGroup.getNumGroups();

        final FastBitSet groupsWithCurrentTerm = new FastBitSet(numGroups);
        final int[] groupToDelta = new int[numGroups];
        final Map<String, Long2ObjectMap<Pair<IntArrayList, IntArrayList>>> intFields = Maps.newHashMap();
        final Map<String, Map<String, Pair<IntArrayList, IntArrayList>>> stringFields = Maps.newHashMap();
        for (int i = 0; i < groups.length; i++) {
            //if the last group(s) exist on other shards but not this one docIdToGroup.getNumGroups() is wrong
            if (groups[i] >= groupToDelta.length) {
                continue;
            }
            final RegroupCondition condition = conditions[i];
            Pair<IntArrayList, IntArrayList> groupDeltas;
            if (condition.intType) {
                Long2ObjectMap<Pair<IntArrayList, IntArrayList>> termToGroupDeltas =
                    intFields.get(condition.field);
                if (termToGroupDeltas == null) {
                    termToGroupDeltas = new Long2ObjectOpenHashMap<>();
                    intFields.put(condition.field, termToGroupDeltas);
                }
                groupDeltas = termToGroupDeltas.get(condition.intTerm);
                if (groupDeltas == null) {
                    groupDeltas = Pair.of(new IntArrayList(), new IntArrayList());
                    termToGroupDeltas.put(condition.intTerm, groupDeltas);
                }
            } else {
                Map<String, Pair<IntArrayList, IntArrayList>> termToGroupDeltas =
                    stringFields.get(condition.field);
                if (termToGroupDeltas == null) {
                    termToGroupDeltas = Maps.newHashMap();
                    stringFields.put(condition.field, termToGroupDeltas);
                }
                groupDeltas = termToGroupDeltas.get(condition.stringTerm);
                if (groupDeltas == null) {
                    groupDeltas = Pair.of(new IntArrayList(), new IntArrayList());
                    termToGroupDeltas.put(condition.stringTerm, groupDeltas);
                }
            }
            groupDeltas.getFirst().add(groups[i]);
            groupDeltas.getSecond().add(deltas[i]);
        }

        try (final DocIdStream docIdStream = flamdexReader.getDocIdStream()){
            for (final Map.Entry<String, Long2ObjectMap<Pair<IntArrayList, IntArrayList>>> entry :
                     intFields.entrySet()) {
                final String field = entry.getKey();
                final Long2ObjectMap<Pair<IntArrayList, IntArrayList>> termToGroupDeltas = entry.getValue();
                try (final IntTermIterator intTermIterator = flamdexReader.getUnsortedIntTermIterator(field)) {
                    for (final Long2ObjectMap.Entry<Pair<IntArrayList, IntArrayList>> entry2 :
                            termToGroupDeltas.long2ObjectEntrySet()) {
                        for (int i = 0; i < groupsSet.size(); i++) {
                            groupsWithCurrentTerm.clear(groupsSet.getInt(i));
                        }
                        groupsSet.clear();
                        final long term = entry2.getLongKey();
                        final Pair<IntArrayList, IntArrayList> groupDeltas = entry2.getValue();
                        final IntArrayList termGroups = groupDeltas.getFirst();
                        final IntArrayList termDeltas = groupDeltas.getSecond();
                        for (int i = 0; i < termGroups.size(); i++) {
                            final int group = termGroups.getInt(i);
                            groupsWithCurrentTerm.set(group);
                            groupToDelta[group] = termDeltas.getInt(i);
                            groupsSet.add(group);
                        }
                        intTermIterator.reset(term);
                        if (!intTermIterator.next()) {
                            continue;
                        }
                        if (intTermIterator.term() != term) {
                            continue;
                        }
                        docIdStream.reset(intTermIterator);
                        updateDocsWithTermDynamicMetric(docIdToGroup, metric, groupsWithCurrentTerm,
                                groupToDelta, docIdStream);
                    }
                }
            }
            for (final Map.Entry<String, Map<String, Pair<IntArrayList, IntArrayList>>> entry :
                     stringFields.entrySet()) {
                final String field = entry.getKey();
                final Map<String, Pair<IntArrayList, IntArrayList>> termToGroupDeltas = entry.getValue();
                try (final StringTermIterator stringTermIterator = flamdexReader.getStringTermIterator(field)) {
                    for (final Map.Entry<String, Pair<IntArrayList, IntArrayList>> entry2 :
                            termToGroupDeltas.entrySet()) {
                        for (int i = 0; i < groupsSet.size(); i++) {
                            groupsWithCurrentTerm.clear(groupsSet.getInt(i));
                        }
                        groupsSet.clear();
                        final String term = entry2.getKey();
                        final Pair<IntArrayList, IntArrayList> groupDeltas = entry2.getValue();
                        final IntArrayList termGroups = groupDeltas.getFirst();
                        final IntArrayList termDeltas = groupDeltas.getSecond();
                        for (int i = 0; i < termGroups.size(); i++) {
                            final int group = termGroups.getInt(i);
                            groupsWithCurrentTerm.set(group);
                            groupToDelta[group] = termDeltas.getInt(i);
                            groupsSet.add(group);
                        }
                        stringTermIterator.reset(term);
                        if (!stringTermIterator.next()) {
                            continue;
                        }
                        if (!stringTermIterator.term().equals(term)) {
                            continue;
                        }
                        docIdStream.reset(stringTermIterator);
                        updateDocsWithTermDynamicMetric(docIdToGroup, metric, groupsWithCurrentTerm, groupToDelta, docIdStream);
                    }
                }
            }
        }
    }

    private void updateDocsWithTermDynamicMetric(
            final GroupLookup docIdToGroup,
            final DynamicMetric metric,
            final FastBitSet groupsWithCurrentTerm,
            final int[] groupToDelta,
            final DocIdStream docIdStream) {
        final int[] docIdBuf = memoryPool.getIntBuffer(BUFFER_SIZE, true);
        final int[] docGroupBuffer = memoryPool.getIntBuffer(BUFFER_SIZE, true);
        while (true) {
            final int n = docIdStream.fillDocIdBuffer(docIdBuf);
            groupAdjustDeltas(docIdToGroup, metric, groupsWithCurrentTerm, groupToDelta, docIdBuf, docGroupBuffer, n);
            if (n < docIdBuf.length) {
                memoryPool.returnIntBuffer(docIdBuf);
                memoryPool.returnIntBuffer(docGroupBuffer);
                break;
            }
        }
    }

    @Override
    public void groupQueryUpdateDynamicMetric(
            final String groupsName,
            final String name,
            final int[] groups,
            final Query[] conditions,
            final int[] deltas) throws ImhotepOutOfMemoryException {
        if (groups.length != conditions.length) {
            throw newIllegalArgumentException("groups and conditions must be the same length");
        }
        validateQueryUpdateDynamicMetricInput(conditions, deltas);
        final DynamicMetric metric = getDynamicMetrics().get(name);
        if (metric == null) {
            throw newRuntimeException("dynamic metric \"" + name + "\" does not exist");
        }
        final IntArrayList groupsSet = new IntArrayList();

        final GroupLookup docIdToGroup = namedGroupLookups.get(groupsName);

        final int[] groupToDelta = new int[docIdToGroup.getNumGroups()];
        final Map<Query, Pair<IntArrayList, IntArrayList>> queryToGroupDeltas = Maps.newHashMap();
        for (int i = 0; i < groups.length; i++) {
            //if the last group(s) exist on other shards but not this one docIdToGroup.getNumGroups() is wrong
            if (groups[i] >= groupToDelta.length) {
                continue;
            }
            final Query condition = conditions[i];
            Pair<IntArrayList, IntArrayList> groupDeltas;
            groupDeltas = queryToGroupDeltas.get(condition);
            if (groupDeltas == null) {
                groupDeltas = Pair.of(new IntArrayList(), new IntArrayList());
                queryToGroupDeltas.put(condition, groupDeltas);
            }
            groupDeltas.getFirst().add(groups[i]);
            groupDeltas.getSecond().add(deltas[i]);
        }

        final FastBitSetPooler bitSetPooler = new ImhotepBitSetPooler(memory);
        FastBitSet bitSet = null;
        FastBitSet groupsWithCurrentTerm = null;

        try {
            bitSet = bitSetPooler.create(flamdexReader.getNumDocs());
            groupsWithCurrentTerm = bitSetPooler.create(docIdToGroup.getNumGroups());
            final FlamdexSearcher searcher = new FlamdexSearcher(flamdexReader);
            for (final Map.Entry<Query, Pair<IntArrayList, IntArrayList>> entry :
                    queryToGroupDeltas.entrySet()) {
                for (int i = 0; i < groupsSet.size(); i++) {
                    groupsWithCurrentTerm.clear(groupsSet.getInt(i));
                }
                groupsSet.clear();
                final Pair<IntArrayList, IntArrayList> groupDeltas = entry.getValue();
                final IntArrayList termGroups = groupDeltas.getFirst();
                final IntArrayList termDeltas = groupDeltas.getSecond();
                for (int i = 0; i < termGroups.size(); i++) {
                    final int group = termGroups.getInt(i);
                    groupsWithCurrentTerm.set(group);
                    groupToDelta[group] = termDeltas.getInt(i);
                    groupsSet.add(group);
                }
                final Query query = entry.getKey();
                searcher.search(query, bitSet, bitSetPooler);

                final FastBitSet.IntIterator iterator = bitSet.iterator();
                final int[] docIdBuf = memoryPool.getIntBuffer(BUFFER_SIZE, true);
                final int[] docGroupBuffer = memoryPool.getIntBuffer(BUFFER_SIZE, true);
                while (true) {
                    int n;
                    for (n = 0; n < docIdBuf.length; n++) {
                        if (!iterator.next()) {
                            break;
                        }
                        docIdBuf[n] = iterator.getValue();
                    }
                    groupAdjustDeltas(docIdToGroup, metric, groupsWithCurrentTerm, groupToDelta, docIdBuf, docGroupBuffer, n);
                    if (n < docIdBuf.length) {
                        memoryPool.returnIntBuffer(docIdBuf);
                        memoryPool.returnIntBuffer(docGroupBuffer);
                        break;
                    }
                }
            }
        } catch (final FlamdexOutOfMemoryException e) {
            throw newImhotepOutOfMemoryException(e);
        } finally {
            if (groupsWithCurrentTerm != null) {
                bitSetPooler.release(groupsWithCurrentTerm.memoryUsage());
            }
            if (bitSet != null) {
                bitSetPooler.release(bitSet.memoryUsage());
            }
        }
    }

    private void groupAdjustDeltas(
            final GroupLookup docIdToGroup,
            final DynamicMetric metric,
            final FastBitSet groupsWithCurrentTerm,
            final int[] groupToDelta,
            final int[] docIdBuf,
            final int[] docGroupBuffer,
            final int n) {
        docIdToGroup.fillDocGrpBuffer(docIdBuf, docGroupBuffer, n);
        final DynamicMetric.Editor editor = metric.getEditor();
        for (int i = 0; i < n; i++) {
            if (groupsWithCurrentTerm.get(docGroupBuffer[i])) {
                editor.add(docIdBuf[i], groupToDelta[docGroupBuffer[i]]);
            }
        }
    }

    private synchronized void adjustDeltas(
            final DynamicMetric metric,
            final DocIdStream docIdStream,
            final int delta,
            final int[] docIdBuf) {
        final DynamicMetric.Editor editor = metric.getEditor();
        while (true) {
            final int n = docIdStream.fillDocIdBuffer(docIdBuf);
            for (int i = 0; i < n; i++) {
                editor.add(docIdBuf[i], delta);
            }
            if (n != docIdBuf.length) {
                break;
            }
        }
    }

    @Override
    public synchronized void close() {
        if (!closed) {
            try {
                tryClose();
            } finally {
                closed = true;
            }
        }
    }

    protected void tryClose() {
        try {
            instrumentation.fire(new CloseLocalSessionEvent());

            while (metricStack.getNumStats() > 0) {
                popStat();
            }
            // not closed in finally because this impacts memory usage and the leak logging below
            Closeables2.closeQuietly(namedGroupLookups, log);

            long dynamicMetricUsage = 0;
            for (final DynamicMetric metric : getDynamicMetrics().values()) {
                dynamicMetricUsage += metric.memoryUsed();
            }
            getDynamicMetrics().clear();
            if (dynamicMetricUsage > 0) {
                memory.releaseMemory(dynamicMetricUsage);
            }
            if (memory.usedMemory() > 0) {
                log.error("ImhotepLocalSession [" + getSessionId() + "] is leaking! memory reserved after " +
                          "all memory has been freed: " + memory.usedMemory());
            }
        } finally {
            Closeables2.closeQuietly(flamdexReaderRef, log);
            Closeables2.closeQuietly(memory, log);
            Closeables2.closeQuietly(metricStack, log);
        }
    }

    @Override
    protected void finalize() {
        if (!closed) {
            log.error("ImhotepLocalSession [" + getSessionId() + "] was not closed!!!!!!! stack trace at construction:",
                      constructorStackTrace);
            close();
        }
    }

    @Override
    public synchronized void resetGroups(final String groupsName) throws ImhotepOutOfMemoryException {
        resetGroupsTo(groupsName, 1);
    }

    protected void resetGroupsTo(final String groupsName, final int group) throws ImhotepOutOfMemoryException {
        GroupLookup lookup = new ConstantGroupLookup(group, numDocs);
        if (group != 0) {
            lookup = moveDeletedDocumentsToGroupZero(lookup);
        }
        namedGroupLookups.claimMemoryAndPut(groupsName, lookup);
    }

    static void clear(final long[] array, final int[] groupsSeen, final int groupsSeenCount) {
        for (int i = 0; i < groupsSeenCount; i++) {
            array[groupsSeen[i]] = 0;
        }
    }

    private static class IntFieldConditionSummary {
        long maxInequalityTerm = Long.MIN_VALUE;
        final Set<Long> otherTerms = new HashSet<>();
    }

    private static class StringFieldConditionSummary {
        String maxInequalityTerm = null;
        final Set<String> otherTerms = new HashSet<>();
    }

    private void applyIntConditions(final GroupLookup docIdToGroup,
                                    final GroupRemapRule[] remapRules,
                                    final DocIdStream docIdStream,
                                    final ThreadSafeBitSet docRemapped) {
        final Map<String, IntFieldConditionSummary> intFields =
                buildIntFieldConditionSummaryMap(remapRules);
        for (final String intField : intFields.keySet()) {
            final IntFieldConditionSummary summary = intFields.get(intField);
            log.debug("[" + getSessionId() + "] Splitting groups using int field: " + intField);
            try (final IntTermIterator itr = flamdexReader.getUnsortedIntTermIterator(intField)) {
                final int[] docIdBuf = memoryPool.getIntBuffer(BUFFER_SIZE, true);

                if (summary.maxInequalityTerm >= 0) {
                    while (itr.next()) {
                        final long itrTerm = itr.term();
                        if (itrTerm > summary.maxInequalityTerm
                                && !summary.otherTerms.contains(itrTerm)) {
                            continue;
                        }
                        docIdStream.reset(itr);
                        do {
                            final int n = docIdStream.fillDocIdBuffer(docIdBuf);
                            docIdToGroup.applyIntConditionsCallback(n,
                                    docIdBuf,
                                    docRemapped,
                                    remapRules,
                                    intField,
                                    itrTerm);
                            if (n != docIdBuf.length) {
                                break;
                            }
                        } while (true);
                    }
                } else {
                    for (final long term : summary.otherTerms) {
                        itr.reset(term);
                        if (itr.next() && itr.term() == term) {
                            docIdStream.reset(itr);
                            do {
                                final int n = docIdStream.fillDocIdBuffer(docIdBuf);
                                docIdToGroup.applyIntConditionsCallback(n,
                                        docIdBuf,
                                        docRemapped,
                                        remapRules,
                                        intField,
                                        term);
                                if (n != docIdBuf.length) {
                                    break;
                                }
                            } while (true);
                        }
                    }
                }
                memoryPool.returnIntBuffer(docIdBuf);
            }
        }
    }

    private void applyStringConditions(final GroupLookup docIdToGroup,
                                       final GroupRemapRule[] remapRules,
                                       final DocIdStream docIdStream,
                                       final ThreadSafeBitSet docRemapped) {
        final Map<String, StringFieldConditionSummary> stringFields =
                buildStringFieldConditionSummaryMap(remapRules);
        for (final String stringField : stringFields.keySet()) {
            final StringFieldConditionSummary summary = stringFields.get(stringField);
            log.debug("[" + getSessionId() + "] Splitting groups using string field: " + stringField);

            try (final StringTermIterator itr = flamdexReader.getStringTermIterator(stringField)) {
                final int[] docIdBuf = memoryPool.getIntBuffer(BUFFER_SIZE, true);

                if (summary.maxInequalityTerm != null) {
                    while (itr.next()) {
                        final String itrTerm = itr.term();
                        if ((summary.maxInequalityTerm.compareTo(itrTerm) < 0)
                                && !summary.otherTerms.contains(itrTerm)) {
                            continue;
                        }
                        docIdStream.reset(itr);
                        do {
                            final int n = docIdStream.fillDocIdBuffer(docIdBuf);
                            docIdToGroup.applyStringConditionsCallback(n,
                                    docIdBuf,
                                    docRemapped,
                                    remapRules,
                                    stringField,
                                    itrTerm);
                            if (n != docIdBuf.length) {
                                break;
                            }
                        } while (true);
                    }
                } else {
                    for (final String term : summary.otherTerms) {
                        itr.reset(term);
                        if (itr.next() && itr.term().equals(term)) {
                            docIdStream.reset(itr);
                            do {
                                final int n = docIdStream.fillDocIdBuffer(docIdBuf);
                                docIdToGroup.applyStringConditionsCallback(n,
                                        docIdBuf,
                                        docRemapped,
                                        remapRules,
                                        stringField,
                                        term);
                                if (n != docIdBuf.length) {
                                    break;
                                }
                            } while (true);
                        }
                    }
                }
                memoryPool.returnIntBuffer(docIdBuf);
            }
        }
    }

    static boolean checkStringCondition(final RegroupCondition condition,
                                        final String stringField,
                                        final String itrTerm) {
        if (condition == null) {
            return true;
        }
        if (condition.intType) {
            return true;
        }
        // field is interned
        if (stringField != condition.field) {
            return true;
        }
        if (condition.inequality) {
            if (itrTerm.compareTo(condition.stringTerm) > 0) {
                return true;
            }
        } else {
            if (!itrTerm.equals(condition.stringTerm)) {
                return true;
            }
        }
        return false;
    }

    static boolean checkIntCondition(
            final RegroupCondition condition,
            final String intField,
            final long itrTerm) {
        if (condition == null) {
            return true;
        }
        if (!condition.intType) {
            return true;
        }
        // field is interned
        if (intField != condition.field) {
            return true;
        }
        if (condition.inequality) {
            if (itrTerm > condition.intTerm) {
                return true;
            }
        } else {
            if (itrTerm != condition.intTerm) {
                return true;
            }
        }
        return false;
    }

    private static Map<String, IntFieldConditionSummary> buildIntFieldConditionSummaryMap(final GroupRemapRule[] rules) {
        final Map<String, IntFieldConditionSummary> ret =
                new HashMap<>();
        for (final GroupRemapRule rule : rules) {
            if (rule == null) {
                continue;
            }
            final RegroupCondition condition = rule.condition;
            if (condition == null) {
                continue;
            }
            if (!condition.intType) {
                continue;
            }
            if (!condition.inequality) {
                continue;
            }

            IntFieldConditionSummary entry = ret.get(condition.field);
            if (entry == null) {
                entry = new IntFieldConditionSummary();
                ret.put(condition.field, entry);
            }
            entry.maxInequalityTerm = Math.max(entry.maxInequalityTerm, condition.intTerm);
        }
        for (final GroupRemapRule rule : rules) {
            if (rule == null) {
                continue;
            }
            final RegroupCondition condition = rule.condition;
            if (condition == null) {
                continue;
            }
            if (!condition.intType) {
                continue;
            }
            if (condition.inequality) {
                continue;
            }

            IntFieldConditionSummary entry = ret.get(condition.field);
            if (entry == null) {
                entry = new IntFieldConditionSummary();
                ret.put(condition.field, entry);
            }
            if (condition.intTerm <= entry.maxInequalityTerm) {
                continue;
            }
            entry.otherTerms.add(condition.intTerm);
        }
        return ret;
    }

    private static Map<String, StringFieldConditionSummary> buildStringFieldConditionSummaryMap(final GroupRemapRule[] rules) {
        final Map<String, StringFieldConditionSummary> ret =
                new HashMap<>();
        for (final GroupRemapRule rule : rules) {
            if (rule == null) {
                continue;
            }
            final RegroupCondition condition = rule.condition;
            if (condition == null) {
                continue;
            }
            if (condition.intType) {
                continue;
            }
            if (!condition.inequality) {
                continue;
            }

            StringFieldConditionSummary entry = ret.get(condition.field);
            if (entry == null) {
                entry = new StringFieldConditionSummary();
                ret.put(condition.field, entry);
            }
            entry.maxInequalityTerm = stringMax(entry.maxInequalityTerm, condition.stringTerm);
        }
        for (final GroupRemapRule rule : rules) {
            if (rule == null) {
                continue;
            }
            final RegroupCondition condition = rule.condition;
            if (condition == null) {
                continue;
            }
            if (condition.intType) {
                continue;
            }
            if (condition.inequality) {
                continue;
            }

            StringFieldConditionSummary entry = ret.get(condition.field);
            if (entry == null) {
                entry = new StringFieldConditionSummary();
                ret.put(condition.field, entry);
            }
            if (entry.maxInequalityTerm != null
                    && condition.stringTerm.compareTo(entry.maxInequalityTerm) <= 0) {
                continue;
            }
            entry.otherTerms.add(condition.stringTerm);
        }
        return ret;
    }

    private static String stringMax(final String a, final String b) {
        if (a == null) {
            return b;
        }
        if (b == null) {
            return a;
        }
        if (a.compareTo(b) >= 0) {
            return a;
        }
        return b;
    }

    private enum Metric {
        COUNT("count()"),
        CACHED("cached()"),
        ADD("+"),
        SUBTRACT("-"),
        MULTIPLY("*"),
        DIVIDE("/"),
        MODULUS("%"),
        ABSOLUTE_VALUE("abs()"),
        MIN("min()"),
        MAX("max()"),
        EQ("="),
        NE("!="),
        LT("<"),
        LTE("<="),
        GT(">"),
        GTE(">="),
        DOCID("docId()");

        private final String key;

        Metric(final String key) {
            this.key = key;
        }

        private static final Map<String, Metric> map;
        static {
            final ImmutableMap.Builder<String, Metric> builder = ImmutableMap.builder();
            for (final Metric metric : Metric.values()) {
                builder.put(metric.key, metric);
            }
            map = builder.build();
        }

        static Metric getMetric(final String statName) {
            return map.get(statName);
        }
    }

    private IntValueLookup hasIntTermFilter(final String field, final long term)
        throws ImhotepOutOfMemoryException {
        final long memoryUsage = getBitSetMemoryUsage();

        if (!memory.claimMemory(memoryUsage)) {
            throw newImhotepOutOfMemoryException();
        }

        return new BitSetIntValueLookup(FlamdexUtils.cacheHasIntTerm(field, term, flamdexReader),
                                        memoryUsage);
    }

    private IntValueLookup hasStringTermFilter(final String field, final String term)
        throws ImhotepOutOfMemoryException {
        final long memoryUsage = getBitSetMemoryUsage();

        if (!memory.claimMemory(memoryUsage)) {
            throw newImhotepOutOfMemoryException();
        }

        return new BitSetIntValueLookup(
                                        FlamdexUtils.cacheHasStringTerm(field, term, flamdexReader),
                                        memoryUsage);
    }

    private IntValueLookup hasIntFieldFilter(final String field) throws ImhotepOutOfMemoryException {
        if (FlamdexUtils.hasZeroTermDoc(flamdexReader, field, true) == Boolean.FALSE) {
            return new Constant(1);
        }

        final long memoryUsage = getBitSetMemoryUsage();

        if (!memory.claimMemory(memoryUsage)) {
            throw newImhotepOutOfMemoryException();
        }

        return new BitSetIntValueLookup(
                FlamdexUtils.cacheHasIntField(field, flamdexReader),
                memoryUsage
        );
    }

    private IntValueLookup hasStringFieldFilter(final String field) throws ImhotepOutOfMemoryException {
        if (FlamdexUtils.hasZeroTermDoc(flamdexReader, field, false) == Boolean.FALSE) {
            return new Constant(1);
        }

        final long memoryUsage = getBitSetMemoryUsage();

        if (!memory.claimMemory(memoryUsage)) {
            throw newImhotepOutOfMemoryException();
        }

        return new BitSetIntValueLookup(
                FlamdexUtils.cacheHasStringField(field, flamdexReader),
                memoryUsage
        );
    }

    private IntValueLookup hasRegexFilter(final String field, final String regex) throws ImhotepOutOfMemoryException {
        final long memoryUsage = getBitSetMemoryUsage();

        if (!memory.claimMemory(memoryUsage)) {
            throw newImhotepOutOfMemoryException();
        }

        return new BitSetIntValueLookup(
                FlamdexUtils.cacheRegex(field, regex, flamdexReader),
                memoryUsage
        );
    }

    private IntValueLookup fieldEqualFilter(final String field1, final String field2) throws ImhotepOutOfMemoryException {
        final long memoryUsage = getBitSetMemoryUsage();

        if (!memory.claimMemory(memoryUsage)) {
            throw newImhotepOutOfMemoryException();
        }

        return new BitSetIntValueLookup(
                FlamdexUtils.cacheFieldEqual(field1, field2, flamdexReader),
                memoryUsage
        );
    }

    private IntValueLookup matchByRegex(final String field, final String regex, final int matchIndex) throws ImhotepOutOfMemoryException {
        final long memoryUsage = 8 * flamdexReader.getNumDocs();

        if (!memory.claimMemory(memoryUsage)) {
            throw newImhotepOutOfMemoryException();
        }

        return new MemoryReservingIntValueLookupWrapper(FlamdexUtils.cacheRegExpCapturedLong(field, flamdexReader, Pattern.compile(regex), matchIndex));
    }

    private IntValueLookup termCountLookup(
            final String field,
            final boolean isIntOperator)
            throws ImhotepOutOfMemoryException {
        // operation(fieldType) -> requested iterator type
        // -----------------------------------------------
        // intTermCount(intField) -> intIterator
        // intTermCount(strField) -> intIterator (will be converted to int into flamdex and will return only valid int terms)
        // strTermCount(strField) -> strIterator
        // strTermCount(intField) -> 0 for some reason (see IMTEPD-455 for details)

        final boolean isIntField = flamdexReader.getIntFields().contains(field);
        final boolean isStringField = flamdexReader.getStringFields().contains(field);

        if ((!isIntField && !isStringField) ||
                (!isIntOperator && isIntField && !isStringField)) {
            // no such field or strTermCount(intField), result is 0 in both cases.
            return new Constant(0);
        }

        final long memoryUsage = flamdexReader.getNumDocs();

        if (!memory.claimMemory(memoryUsage)) {
            throw newImhotepOutOfMemoryException();
        }

        final byte[] array = new byte[flamdexReader.getNumDocs()];

        try (final TermIterator iterator = isIntOperator ?
                flamdexReader.getUnsortedIntTermIterator(field) : flamdexReader.getStringTermIterator(field)) {
            try (final DocIdStream docIdStream = flamdexReader.getDocIdStream()) {
                final int[] docIdBuf = memoryPool.getIntBuffer(BUFFER_SIZE, true);
                while (iterator.next()) {
                    docIdStream.reset(iterator);
                    while (true) {
                        final int n = docIdStream.fillDocIdBuffer(docIdBuf);
                        for (int i = 0; i < n; ++i) {
                            final int doc = docIdBuf[i];
                            if (array[doc] != (byte) 255) {
                                ++array[doc];
                            }
                        }
                        if (n < BUFFER_SIZE) {
                            break;
                        }
                    }
                }
                memoryPool.returnIntBuffer(docIdBuf);
            }
        }

        return new MemoryReservingIntValueLookupWrapper(new ByteArrayIntValueLookup(array, 0, 255));
    }

    private IntValueLookup stringLenLookup(final String field) throws ImhotepOutOfMemoryException {
        final long memoryUsage = 2 * flamdexReader.getNumDocs();

        if (!memory.claimMemory(memoryUsage)) {
            throw newImhotepOutOfMemoryException();
        }

        final char[] result = new char[flamdexReader.getNumDocs()];
        char max = Character.MAX_VALUE;
        char min = Character.MIN_VALUE;

        try (StringTermIterator iterator = flamdexReader.getStringTermIterator(field);
                    DocIdStream docIdStream = flamdexReader.getDocIdStream()) {
            final int[] docIdBuf = memoryPool.getIntBuffer(BUFFER_SIZE, true);
            while (iterator.next()) {
                final char len = (char) Math.min(iterator.term().length(), Character.MAX_VALUE);
                min = (char) Math.min(min, len);
                max = (char) Math.max(max, len);
                docIdStream.reset(iterator);
                while (true) {
                    final int n = docIdStream.fillDocIdBuffer(docIdBuf);
                    for (int i = 0; i < n; ++i) {
                        final int doc = docIdBuf[i];
                        if (result[doc] != 0) {
                            memory.releaseMemory(memoryUsage);
                            throw new MultiValuedFieldStringLenException(createMessageWithSessionId("String len operator is not supported for multi-valued fields"));
                        }
                        result[doc] = len;
                    }
                    if (n < docIdBuf.length) {
                        break;
                    }
                }
            }
            memoryPool.returnIntBuffer(docIdBuf);
        }

        return new MemoryReservingIntValueLookupWrapper(new CharArrayIntValueLookup(result, min, max));
    }

    private static int parseAndRound(final String term, final double scale, final double offset) {
        int result;
        try {
            final double termFloat = Double.parseDouble(term);
            result = (int) Math.round(termFloat * scale + offset);
        } catch (final NumberFormatException e) {
            result = 0;
        }
        return result;
    }

    private IntValueLookup scaledFloatLookup(final String field, final double scale, final double offset)
        throws ImhotepOutOfMemoryException {
        final long memoryUsage = 4 * flamdexReader.getNumDocs();

        if (!memory.claimMemory(memoryUsage)) {
            throw newImhotepOutOfMemoryException();
        }

        final int[] array = new int[flamdexReader.getNumDocs()];
        int min = Integer.MAX_VALUE;
        int max = Integer.MIN_VALUE;
        final StringTermDocIterator iterator = flamdexReader.getStringTermDocIterator(field);
        try {
            final int[] docIdBuf = memoryPool.getIntBuffer(BUFFER_SIZE, true);
            while (iterator.nextTerm()) {
                final String term = iterator.term();
                final int number = parseAndRound(term, scale, offset);
                min = Math.min(min, number);
                max = Math.max(max, number);

                while (true) {
                    final int n = iterator.fillDocIdBuffer(docIdBuf);
                    for (int i = 0; i < n; i++) {
                        final int doc = docIdBuf[i];
                        array[doc] = number;
                    }
                    if (n < BUFFER_SIZE) {
                        break;
                    }
                }
            }
            memoryPool.returnIntBuffer(docIdBuf);
        } finally {
            Closeables2.closeQuietly(iterator, log);
        }

        return new MemoryReservingIntValueLookupWrapper(new IntArrayIntValueLookup(array, min, max));
    }

    private IntValueLookup randomLookup(final String field, final boolean isIntField, final String salt, final double[] percentages) throws ImhotepOutOfMemoryException {
        ensurePercentagesValidity(percentages);

        // TODO: Size this based on percentages.length + 1. No need to have a full int[].
        final long memoryUsage = 4 * flamdexReader.getNumDocs();
        if (!memory.claimMemory(memoryUsage)) {
            throw newImhotepOutOfMemoryException();
        }
        final int[] array = new int[flamdexReader.getNumDocs()];

        try(final IterativeHasherUtils.TermHashIterator iterator =
                    IterativeHasherUtils.create(flamdexReader, field, isIntField, salt)) {
            final IterativeHasherUtils.GroupChooser groupChooser =
                    IterativeHasherUtils.createChooser(percentages);

            final int[] docIdBuf = memoryPool.getIntBuffer(BUFFER_SIZE, true);
            try {
                while (iterator.hasNext()) {
                    final int hash = iterator.getHash();
                    // Use zero as the not-present value for ease of use
                    final int value = groupChooser.getGroup(hash) + 1;

                    final DocIdStream docIdStream = iterator.getDocIdStream();

                    while (true) {
                        final int n = docIdStream.fillDocIdBuffer(docIdBuf);
                        for (int i = 0; i < n; i++) {
                            array[docIdBuf[i]] = value;
                        }
                        if (n < BUFFER_SIZE) {
                            break;
                        }
                    }
                }
            } finally {
                memoryPool.returnIntBuffer(docIdBuf);
            }
        }

        return new MemoryReservingIntValueLookupWrapper(new IntArrayIntValueLookup(array, 0, percentages.length + 1));
    }

    private IntValueLookup randomMetricLookup(final IntValueLookup lookup, final String salt, final double[] percentages) throws ImhotepOutOfMemoryException {
        ensurePercentagesValidity(percentages);

        // TODO: Size this based on percentages.length + 1. No need to have a full int[].
        final long memoryUsage = 4 * flamdexReader.getNumDocs();
        if (!memory.claimMemory(memoryUsage)) {
            throw newImhotepOutOfMemoryException();
        }
        final int[] array = new int[flamdexReader.getNumDocs()];

        // Using ConsistentLongHasher to be consistent with randomMetricRegroup to the extent that we can.
        final IterativeHasher.ConsistentLongHasher hasher = new IterativeHasher.Murmur3Hasher(salt).consistentLongHasher();
        final IterativeHasherUtils.GroupChooser chooser = IterativeHasherUtils.createChooser(percentages);

        final int[] docIdBuf = memoryPool.getIntBuffer(BUFFER_SIZE, true);
        final long[] valBuf = memoryPool.getLongBuffer(BUFFER_SIZE, true);
        try {
            for (int startDoc = 0; startDoc < numDocs; startDoc += BUFFER_SIZE) {
                final int n = Math.min(BUFFER_SIZE, numDocs - startDoc);
                for (int i = 0; i < n; i++) {
                    docIdBuf[i] = startDoc + i;
                }
                lookup.lookup(docIdBuf, valBuf, n);
                for (int i = 0; i < n; i++) {
                    final int hash = hasher.calculateHash(valBuf[i]);
                    final int value = chooser.getGroup(hash) + 1;
                    array[docIdBuf[i]] = value;
                }
            }
        } finally {
            memoryPool.returnIntBuffer(docIdBuf);
            memoryPool.returnLongBuffer(valBuf);
        }

        return new MemoryReservingIntValueLookupWrapper(new IntArrayIntValueLookup(array, 0, percentages.length + 1));
    }

    private int getBitSetMemoryUsage() {
        return flamdexReader.getNumDocs() / 8 + ((flamdexReader.getNumDocs() % 8) != 0 ? 1 : 0);
    }

    private class BitSetIntValueLookup implements IntValueLookup {
        private ThreadSafeBitSet bitSet;
        private long memoryUsage;

        private BitSetIntValueLookup(final ThreadSafeBitSet bitSet, final long memoryUsage) {
            this.bitSet = bitSet;
            this.memoryUsage = memoryUsage;
        }

        @Override
        public long getMin() {
            return 0;
        }

        @Override
        public long getMax() {
            return 1;
        }

        @Override
        public void lookup(final int[] docIds, final long[] values, final int n) {
            for (int i = 0; i < n; ++i) {
                values[i] = bitSet.get(docIds[i]) ? 1 : 0;
            }
        }

        @Override
        public long memoryUsed() {
            return 0;
        }

        @Override
        public void close() {
            bitSet = null;
            memory.releaseMemory(memoryUsage);
            memoryUsage = 0;
        }
    }

    private final class MemoryReservingIntValueLookupWrapper implements IntValueLookup {
        final IntValueLookup lookup;
        private boolean closed = false;

        private MemoryReservingIntValueLookupWrapper(final IntValueLookup lookup) {
            this.lookup = lookup;
        }

        @Override
        public long getMin() {
            return lookup.getMin();
        }

        @Override
        public long getMax() {
            return lookup.getMax();
        }

        @Override
        public void lookup(final int[] docIds, final long[] values, final int n) {
            lookup.lookup(docIds, values, n);
        }

        @Override
        public long memoryUsed() {
            return lookup.memoryUsed();
        }

        @Override
        public void close() {
            if (!closed) {
                final long usedMemory = memoryUsed();
                lookup.close();
                memory.releaseMemory(usedMemory);
            }
            closed = true;
        }
    }

    <T> T executeBatchRequest(final List<ImhotepCommand> firstCommands, final ImhotepCommand<T> lastCommand) throws ImhotepOutOfMemoryException {
        for (final ImhotepCommand command: firstCommands) {
            command.apply(this);
        }
        return lastCommand.apply(this);
    }
}

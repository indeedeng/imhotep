package com.indeed.imhotep.multicache;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.indeed.flamdex.api.FlamdexReader;
import com.indeed.flamdex.api.IntValueLookup;
import com.indeed.flamdex.datastruct.FastBitSet;
import com.indeed.flamdex.simple.SimpleFlamdexReader;
import com.indeed.imhotep.AbstractImhotepSession;
import com.indeed.imhotep.GroupMultiRemapRule;
import com.indeed.imhotep.GroupRemapRule;
import com.indeed.imhotep.MemoryReservationContext;
import com.indeed.imhotep.MemoryReserver;
import com.indeed.imhotep.QueryRemapRule;
import com.indeed.imhotep.RegroupCondition;
import com.indeed.imhotep.TermCount;
import com.indeed.imhotep.api.DocIterator;
import com.indeed.imhotep.api.FTGSIterator;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.imhotep.api.RawFTGSIterator;
import com.indeed.imhotep.local.ConstantGroupLookup;
import com.indeed.imhotep.local.DynamicMetric;
import com.indeed.imhotep.local.ImhotepLocalSession;
import com.indeed.util.core.reference.SharedReference;
import org.apache.log4j.Logger;

import java.io.File;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by darren on 3/12/15.
 */
public class MultiShardSession extends AbstractImhotepSession {
    private static final int MAX_NUMBER_STATS = 64;
    private static final Logger log = Logger.getLogger(MultiShardSession.class);

    // do not close flamdexReader, it is separately refcounted
    private List<SimpleFlamdexReader> flamdexReaders;
    private List<SharedReference<SimpleFlamdexReader>> flamdexReaderRefs;

    final MemoryReservationContext memory;

    int[] groupDocCount;

    int numStats;
    final IntValueLookup[] statLookup = new IntValueLookup[MAX_NUMBER_STATS];
    private final List<String> statCommands;

    private final boolean[] needToReCalcGroupStats = new boolean[MAX_NUMBER_STATS];

    private boolean closed = false;
    @VisibleForTesting
    private Map<String, DynamicMetric> dynamicMetrics = Maps.newHashMap();

    private final Exception constructorStackTrace;

    public MultiShardSession(final List<SimpleFlamdexReader> flamdexReaders,
                               final MemoryReservationContext memory) throws ImhotepOutOfMemoryException {
        this.constructorStackTrace = new Exception();
        this.flamdexReaderRefs = Lists.newArrayListWithCapacity(flamdexReaders.size());
        this.flamdexReaders = flamdexReaders;
        for (SimpleFlamdexReader reader : flamdexReaders) {
            this.flamdexReaderRefs.add(SharedReference.create(reader));
        }
        this.memory = memory;

        groupDocCount = clearAndResize((int[]) null, docIdToGroup.getNumGroups(), memory);
        groupDocCount[1] = numDocs;
        this.statCommands = new ArrayList<String>();
    }

    @Override
    public long getTotalDocFreq(String[] intFields, String[] stringFields) {
        return 0;
    }

    @Override
    public long[] getGroupStats(int stat) {
        return new long[0];
    }

    @Override
    public FTGSIterator getFTGSIterator(String[] intFields, String[] stringFields) {
        return null;
    }

    @Override
    public FTGSIterator getSubsetFTGSIterator(Map<String, long[]> intFields,
                                              Map<String, String[]> stringFields) {
        return null;
    }

    @Override
    public RawFTGSIterator[] getSubsetFTGSIteratorSplits(Map<String, long[]> intFields,
                                                         Map<String, String[]> stringFields) {
        return new RawFTGSIterator[0];
    }

    @Override
    public DocIterator getDocIterator(String[] intFields,
                                      String[] stringFields) throws ImhotepOutOfMemoryException {
        return null;
    }

    @Override
    public RawFTGSIterator[] getFTGSIteratorSplits(String[] intFields, String[] stringFields) {
        return new RawFTGSIterator[0];
    }

    @Override
    public RawFTGSIterator getFTGSIteratorSplit(String[] intFields,
                                                String[] stringFields,
                                                int splitIndex,
                                                int numSplits) {
        return null;
    }

    @Override
    public void writeFTGSIteratorSplit(String[] intFields,
                                       String[] stringFields,
                                       int splitIndex,
                                       int numSplits,
                                       Socket socket) {

    }

    @Override
    public RawFTGSIterator getSubsetFTGSIteratorSplit(Map<String, long[]> intFields,
                                                      Map<String, String[]> stringFields,
                                                      int splitIndex,
                                                      int numSplits) {
        return null;
    }

    @Override
    public RawFTGSIterator mergeFTGSSplit(String[] intFields,
                                          String[] stringFields,
                                          String sessionId,
                                          InetSocketAddress[] nodes,
                                          int splitIndex) {
        return null;
    }

    @Override
    public RawFTGSIterator mergeSubsetFTGSSplit(Map<String, long[]> intFields,
                                                Map<String, String[]> stringFields,
                                                String sessionId,
                                                InetSocketAddress[] nodes,
                                                int splitIndex) {
        return null;
    }

    @Override
    public int regroup(GroupMultiRemapRule[] rawRules,
                       boolean errorOnCollisions) throws ImhotepOutOfMemoryException {
        return 0;
    }

    @Override
    public int regroup(GroupRemapRule[] rawRules) throws ImhotepOutOfMemoryException {
        return 0;
    }

    @Override
    public int regroup(QueryRemapRule rule) throws ImhotepOutOfMemoryException {
        return 0;
    }

    @Override
    public void intOrRegroup(String field,
                             long[] terms,
                             int targetGroup,
                             int negativeGroup,
                             int positiveGroup) throws ImhotepOutOfMemoryException {

    }

    @Override
    public void stringOrRegroup(String field,
                                String[] terms,
                                int targetGroup,
                                int negativeGroup,
                                int positiveGroup) throws ImhotepOutOfMemoryException {

    }

    @Override
    public void regexRegroup(String field,
                             String regex,
                             int targetGroup,
                             int negativeGroup,
                             int positiveGroup) throws ImhotepOutOfMemoryException {

    }

    @Override
    public void randomRegroup(String field,
                              boolean isIntField,
                              String salt,
                              double p,
                              int targetGroup,
                              int negativeGroup,
                              int positiveGroup) throws ImhotepOutOfMemoryException {

    }

    @Override
    public void randomMultiRegroup(String field,
                                   boolean isIntField,
                                   String salt,
                                   int targetGroup,
                                   double[] percentages,
                                   int[] resultGroups) throws ImhotepOutOfMemoryException {

    }

    @Override
    public int metricRegroup(int stat,
                             long min,
                             long max,
                             long intervalSize,
                             boolean noGutters) throws ImhotepOutOfMemoryException {
        return 0;
    }

    @Override
    public int metricRegroup2D(int xStat,
                               long xMin,
                               long xMax,
                               long xIntervalSize,
                               int yStat,
                               long yMin,
                               long yMax,
                               long yIntervalSize) throws ImhotepOutOfMemoryException {
        return 0;
    }

    @Override
    public int metricFilter(int stat,
                            long min,
                            long max,
                            boolean negate) throws ImhotepOutOfMemoryException {
        return 0;
    }

    @Override
    public List<TermCount> approximateTopTerms(String field, boolean isIntField, int k) {
        return null;
    }

    @Override
    public int pushStat(String statName) throws ImhotepOutOfMemoryException {
        return 0;
    }

    @Override
    public int pushStats(List<String> statNames) throws ImhotepOutOfMemoryException {
        return 0;
    }

    @Override
    public int popStat() {
        return 0;
    }

    @Override
    public int getNumStats() {
        return 0;
    }

    @Override
    public int getNumGroups() {
        return 0;
    }

    @Override
    public long getLowerBound(int stat) {
        return 0;
    }

    @Override
    public long getUpperBound(int stat) {
        return 0;
    }

    @Override
    public void createDynamicMetric(String name) throws ImhotepOutOfMemoryException {

    }

    @Override
    public void updateDynamicMetric(String name, int[] deltas) throws ImhotepOutOfMemoryException {

    }

    @Override
    public void conditionalUpdateDynamicMetric(String name,
                                               RegroupCondition[] conditions,
                                               int[] deltas) {

    }

    @Override
    public void groupConditionalUpdateDynamicMetric(String name,
                                                    int[] groups,
                                                    RegroupCondition[] conditions,
                                                    int[] deltas) {

    }

    @Override
    public void close() {

    }

    @Override
    public void resetGroups() throws ImhotepOutOfMemoryException {

    }

    @Override
    public void rebuildAndFilterIndexes(List<String> intFields,
                                        List<String> stringFields) throws ImhotepOutOfMemoryException {

    }


    private static int[] clearAndResize(int[] a, int newSize, MemoryReserver memory) throws ImhotepOutOfMemoryException {
        if (a == null || newSize > a.length) {
            if (!memory.claimMemory(newSize * 4)) {
                throw new ImhotepOutOfMemoryException();
            }
            final int[] ret = new int[newSize];
            if (a != null) {
                memory.releaseMemory(a.length * 4);
            }
            return ret;
        }
        Arrays.fill(a, 0);
        return a;
    }

}

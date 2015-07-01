package com.indeed.imhotep.local;

import com.google.common.primitives.Ints;
import com.indeed.flamdex.api.IntValueLookup;
import com.indeed.util.core.sort.Quicksortable;
import com.indeed.util.core.sort.Quicksortables;

import it.unimi.dsi.fastutil.ints.IntArrayFIFOQueue;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;

import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;

/**
 * @author jplaisance
 */
public final class MultiCacheConfig {
    private static final Logger log = Logger.getLogger(MultiCacheConfig.class);

    private StatsOrderingInfo[] ordering;
    private boolean onlyBinaryMetrics = false;

    public static class StatsOrderingInfo {
        public final int originalOrder;
        public final long min;
        public final long max;
        public final int sizeInBytes;
        public final int vectorNum;
        public final int offsetInVector;

        public StatsOrderingInfo(int originalOrder, long min, long max, int sizeInBytes, int vectorNum, int offsetInVector) {
            this.originalOrder = originalOrder;
            this.min = min;
            this.max = max;
            this.sizeInBytes = sizeInBytes;
            this.vectorNum = vectorNum;
            this.offsetInVector = offsetInVector;
        }
    }

    public MultiCacheConfig() { }

    public StatsOrderingInfo[] getOrdering() {
        return this.ordering;
    }

    public boolean isOnlyBinaryMetrics() {
        return onlyBinaryMetrics;
    }

    public void calcOrdering(final StatLookup[] statLookups, final int numStats) {
        this.ordering = calculateMetricOrder(statLookups, numStats);

        for (StatsOrderingInfo info : this.ordering) {
            if (info.sizeInBytes != 0) {
                this.onlyBinaryMetrics = false;
                return;
            }
        }

        this.onlyBinaryMetrics = true;
    }

    private static StatsOrderingInfo[] calculateMetricOrder(final StatLookup[] sessionStats,
                                                            int numStats) {
        final IntArrayList booleanMetrics = new IntArrayList();
        final IntArrayList longMetrics = new IntArrayList();
        final long[] mins = new long[numStats];
        final long[] maxes = new long[numStats];
        final int[] bits = new int[numStats];
        Arrays.fill(mins, Long.MAX_VALUE);
        Arrays.fill(maxes, Long.MIN_VALUE);
        for (StatLookup stats : sessionStats) {
            for (int j = 0; j < numStats; j++) {
                mins[j] = Math.min(mins[j], stats.get(j).getMin());
                maxes[j] = Math.max(maxes[j], stats.get(j).getMax());
            }
        }
        for (int i = 0; i < numStats; i++) {
            final long range = maxes[i] - mins[i];
            bits[i] = range == 0 ? 1 : 64-Long.numberOfLeadingZeros(range);
            if (bits[i] == 1 && booleanMetrics.size() < 4) {
                booleanMetrics.add(i);
            } else {
                longMetrics.add(i);
            }
        }

        // Check if there are only boolean metrics
        if (longMetrics.size() == 0) {
            final StatsOrderingInfo[] ret = new StatsOrderingInfo[numStats];
            int metricIndex = 0;
            for (int i = 0; i < booleanMetrics.size(); i++) {
                final int metric = booleanMetrics.getInt(i);
                ret[metricIndex] = new StatsOrderingInfo(metric, mins[metric], maxes[metric], 0, 0, 0);
                metricIndex++;
            }
            return ret;
        }

        final List<IntList> vectorMetrics;
        // do exhaustive search for up to 10 metrics
        // optimizes first for least number of vectors then least space used for group stats
        // this is impractical beyond 10 due to being O(N!)
        if (longMetrics.size() <= 10) {
            final Permutation bestPermutation = permutations(longMetrics.toIntArray(), new ReduceFunction<int[], Permutation>() {
                @Override
                public Permutation apply(int[] ints, Permutation best) {
                    final Permutation permutation = getPermutation(ints, bits, 4);
                    if (best == null) {
                        return permutation;
                    }
                    if (permutation.vectorsUsed < best.vectorsUsed ||
                            (permutation.vectorsUsed == best.vectorsUsed && permutation.statsSpace < best.statsSpace)) {
                        return permutation;
                    }
                    return best;
                }
            }, null);
            vectorMetrics = bestPermutation.vectorMetrics;
        } else {
            // use sorted best fit approximation for > 10 metrics optimizing for least number of vectors
            Quicksortables.sort(new Quicksortable() {
                @Override
                public void swap(int i, int j) {
                    final int tmp = longMetrics.getInt(i);
                    longMetrics.set(i, longMetrics.getInt(j));
                    longMetrics.set(j, tmp);
                }

                @Override
                public int compare(int i, int j) {
                    return -Ints.compare(bits[longMetrics.getInt(i)], bits[longMetrics.getInt(j)]);
                }
            }, longMetrics.size());
            final IntArrayList spaceRemaining = new IntArrayList();
            final ArrayList<IntArrayList> initialVectorMetrics = new ArrayList<IntArrayList>();
            spaceRemaining.add(12);
            initialVectorMetrics.add(new IntArrayList());
            for (int i = 0; i < longMetrics.size(); i++) {
                int bestIndex = -1;
                int bestRemaining = 16;
                final int metric = longMetrics.getInt(i);
                final int size = (bits[metric]+7)/8;
                for (int j = 0; j < spaceRemaining.size(); j++) {
                    final int remaining = spaceRemaining.getInt(j);
                    if (size <= remaining && remaining < bestRemaining) {
                        bestIndex = j;
                        bestRemaining = remaining;
                    }
                }
                if (bestIndex == -1) {
                    spaceRemaining.add(16-size);
                    final IntArrayList list = new IntArrayList();
                    list.add(metric);
                    initialVectorMetrics.add(list);
                } else {
                    spaceRemaining.set(bestIndex, bestRemaining-size);
                    initialVectorMetrics.get(bestIndex).add(metric);
                }
            }
            final LinkedList<IntArrayList> vectorMetrics2 = new LinkedList<IntArrayList>(initialVectorMetrics);

            boolean first = true;

            vectorMetrics = new ArrayList<IntList>();
            outer: while (!vectorMetrics2.isEmpty()) {
                final IntArrayList list = vectorMetrics2.removeFirst();
                final ListIterator<IntArrayList> iterator = vectorMetrics2.listIterator();
                while (iterator.hasNext()) {
                    final IntArrayList other = iterator.next();
                    if (list.size()+other.size() < 10) {
                        final IntArrayList currentPermutation = new IntArrayList(list);
                        currentPermutation.addAll(other);
                        final int[] permutationInts = currentPermutation.toIntArray();
                        final Permutation initialPermutation = getPermutation(permutationInts, bits, first ? 4 : 0);
                        final boolean finalFirst = first;
                        final Permutation bestPermutation = permutations(currentPermutation.toIntArray(), new ReduceFunction<int[], Permutation>() {
                            @Override
                            public Permutation apply(int[] ints, Permutation best) {
                                final Permutation permutation = getPermutation(ints, bits, finalFirst ? 4 : 0);
                                if (permutation.vectorsUsed == 2 && permutation.statsSpace < best.statsSpace) {
                                    return permutation;
                                }
                                return best;
                            }
                        }, initialPermutation);
                        if (bestPermutation.statsSpace < initialPermutation.statsSpace) {
                            vectorMetrics.addAll(bestPermutation.vectorMetrics);
                            iterator.remove();
                            first = false;
                            continue outer;
                        }
                    }
                }
                vectorMetrics.add(list);
                first = false;
            }
        }
        final List<IntList> sizes = new ArrayList<IntList>();
        for (IntList list : vectorMetrics) {
            final IntList sizeList = new IntArrayList();
            for (int metric : list) {
                sizeList.add((bits[metric]+7)/8);
            }
            sizes.add(sizeList);
        }
//        System.out.println(sizes);
        final StatsOrderingInfo[] ret = new StatsOrderingInfo[numStats];
        int metricIndex = 0;
        for (int i = 0; i < booleanMetrics.size(); i++) {
            final int metric = booleanMetrics.getInt(i);
            ret[metricIndex] = new StatsOrderingInfo(metric, mins[metric], maxes[metric], 0, 0, 0);
            metricIndex++;
        }
        for (int i = 0; i < vectorMetrics.size(); i++) {
            final IntList list = vectorMetrics.get(i);
            int index = (i == 0) ? 4 : 0;
            for (int j = 0; j < list.size(); j++) {
                final int metric = list.get(j);
                final int size = (bits[metric] + 7) / 8;
                ret[metricIndex] = new StatsOrderingInfo(metric, mins[metric], maxes[metric], size, i, index);
                index += size;
                metricIndex++;
            }
        }
        return ret;
    }

    private static <B> B permutations(int[] ints, ReduceFunction<int[],B> f, B initial) {
        final IntArrayFIFOQueue values = new IntArrayFIFOQueue();
        for (int i : ints) {
            values.enqueue(i);
        }
        final int[] permutation = new int[ints.length];
        return permutations(permutation, 0, values, f, initial);
    }

    private static <B> B permutations(int[] permutation, int index, IntArrayFIFOQueue values, ReduceFunction<int[],B> f, B result) {
        if (index == permutation.length) {
            return f.apply(permutation, result);
        }
        for (int i = 0; i < values.size(); i++) {
            final int value = values.dequeueInt();
            permutation[index] = value;
            result = permutations(permutation, index+1, values, f, result);
            values.enqueue(value);
        }
        return result;
    }

    private static interface ReduceFunction<A,B> {
        B apply(A a, B b);
    }

    public static void main(String[] args) {
        final int[] ints = new int[]{1,2,3,4,5,6,7,8,9,10};
        long time = -System.nanoTime();
        permutations(Arrays.copyOf(ints, ints.length), new ReduceFunction<int[], Object>() {
            @Override
            public Object apply(int[] ints, Object o) {
//                System.out.println(Arrays.toString(ints));
                return o;
            }
        }, null);
        time += System.nanoTime();
        System.out.println(time / 1000000d);

        final MultiCacheConfig multiCacher = new MultiCacheConfig();
        IntValueLookup[] iv = new IntValueLookup[]{
                                                   new DummyIntValueLookup(0, 1),
                                                      new DummyIntValueLookup(0, 255),
                                                      new DummyIntValueLookup(0, 1),
                                                      new DummyIntValueLookup(0, 65535),
                                                      new DummyIntValueLookup(0, 1),
                                                      new DummyIntValueLookup(0, Long.MAX_VALUE),
                                                      new DummyIntValueLookup(0, 1),
                                                      new DummyIntValueLookup(0, 1000000),
                                                      new DummyIntValueLookup(0, 1),
                                                      new DummyIntValueLookup(0, Long.MAX_VALUE),
                                                      new DummyIntValueLookup(0, Long.MAX_VALUE),
//                                                    new DummyIntValueLookup(0, Integer.MAX_VALUE),
//                                                    new DummyIntValueLookup(0, Integer.MAX_VALUE),
//                                                    new DummyIntValueLookup(0, Integer.MAX_VALUE),
                                                      new DummyIntValueLookup(0, Integer.MAX_VALUE * 65536L),
                                                      new DummyIntValueLookup(0, Integer.MAX_VALUE * 65536L),
                                                      new DummyIntValueLookup(0, Integer.MAX_VALUE * 65536L),
                                                      new DummyIntValueLookup(0, Integer.MAX_VALUE * 65536L),
                                                      new DummyIntValueLookup(0, Integer.MAX_VALUE * 65536L),
                                                      new DummyIntValueLookup(0, Integer.MAX_VALUE * 65536L),
                                                      new DummyIntValueLookup(0, Integer.MAX_VALUE * 65536L)
        };
        StatLookup sl = new StatLookup(iv.length);
        for (int i = 0; i < iv.length; i++) {
            sl.set(i, iv[i]);
        }
        multiCacher.calcOrdering(new StatLookup[] {sl}, iv.length);

        StatsOrderingInfo[] ordering = multiCacher.ordering;
        int count = ordering.length;

        final long[] mins = new long[count];
        final long[] maxes = new long[count];
        final int[] sizesInBytes = new int[count];
        final int[] vectorNums = new int[count];
        final int[] offsetsInVectors = new int[count];
        final byte[] metricIndex = new byte[count];

        for (int i = 0; i < ordering.length; i++) {
            final MultiCacheConfig.StatsOrderingInfo orderInfo = ordering[i];
            mins[i] = orderInfo.min;
            maxes[i] = orderInfo.max;
            sizesInBytes[i] = orderInfo.sizeInBytes;
            vectorNums[i] = orderInfo.vectorNum;
            offsetsInVectors[i] = orderInfo.offsetInVector;
            metricIndex[orderInfo.originalOrder] = (byte)i;
        }


        System.out.println("Done!");
    }

    private static final class Permutation {
        final List<IntList> vectorMetrics;
        final int vectorsUsed;
        final int statsSpace;

        private Permutation(List<IntList> vectorMetrics, int vectorsUsed, int statsSpace) {
            this.vectorMetrics = vectorMetrics;
            this.vectorsUsed = vectorsUsed;
            this.statsSpace = statsSpace;
        }
    }

    private static Permutation getPermutation(int[] permutation, int[] bits, int start) {
        int vectors = 1;
        int currentVectorStats = 0;
        int index = start;
        int outputStats = 0;
        final List<IntList> vectorMetrics = new ArrayList<IntList>();
        vectorMetrics.add(new IntArrayList());
        for (int metric : permutation) {
            final int metricSize = (bits[metric] + 7) / 8;
            if (index + metricSize > 16 * vectors) {
                outputStats += (currentVectorStats + 1) / 2 * 2;
                currentVectorStats = 0;
                index = 16 * vectors;
                vectors++;
                vectorMetrics.add(new IntArrayList());
            }
            index += metricSize;
            currentVectorStats++;
            vectorMetrics.get(vectorMetrics.size()-1).add(metric);
        }
        outputStats += (currentVectorStats+1)/2*2;
        return new Permutation(vectorMetrics, vectors, outputStats);
    }

    private static final class DummyIntValueLookup implements IntValueLookup {

        final long min;
        final long max;

        private DummyIntValueLookup(long min, long max) {
            this.min = min;
            this.max = max;
        }

        @Override
        public long getMin() {
            return min;
        }

        @Override
        public long getMax() {
            return max;
        }

        @Override
        public void lookup(int[] docIds, long[] values, int n) {
            throw new UnsupportedOperationException();
        }

        @Override
        public long memoryUsed() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void close() {
            throw new UnsupportedOperationException();
        }
    }
}

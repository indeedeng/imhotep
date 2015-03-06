package com.indeed.imhotep.local;

import com.google.common.base.Throwables;
import com.google.common.primitives.Ints;
import com.indeed.flamdex.api.IntValueLookup;
import com.indeed.util.core.sort.Quicksortable;
import com.indeed.util.core.sort.Quicksortables;
import it.unimi.dsi.fastutil.ints.IntArrayFIFOQueue;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;

/**
 * @author jplaisance
 */
public final class MultiCacher {
    private static final Logger log = Logger.getLogger(MultiCacher.class);
    private final CyclicBarrier barrier;
    private IntValueLookup[][] sessionStats;
    private int numStats;
    private long[] mins;
    private long[] maxes;
    private int[] metrics;

    public MultiCacher(int numSessions) {
        this.barrier = new CyclicBarrier(numSessions);
        sessionStats = new IntValueLookup[numSessions][];
    }

    public MultiCache createMultiCache(int sessionIndex, GroupLookup groupLookup, IntValueLookup[] stats, int numStats) {
        final IntValueLookup[] statsCopy = Arrays.copyOf(stats, numStats);
        sessionStats[sessionIndex] = statsCopy;
        this.numStats = numStats;
        final int index;
        try {
            index = barrier.await();
        } catch (InterruptedException e) {
            throw Throwables.propagate(e);
        } catch (BrokenBarrierException e) {
            throw Throwables.propagate(e);
        }
        if (index == 0) {
            calculateMetricOrder();
        }
        try {
            barrier.await();
        } catch (InterruptedException e) {
            throw Throwables.propagate(e);
        } catch (BrokenBarrierException e) {
            throw Throwables.propagate(e);
        }
        //todo call native methods to create metric cache and set stats
        return null;
    }

    private void calculateMetricOrder() {
        final IntArrayList booleanMetrics = new IntArrayList();
        final IntArrayList longMetrics = new IntArrayList();
        final long[] localMins = new long[numStats];
        final long[] localMaxes = new long[numStats];
        final int[] bits = new int[numStats];
        Arrays.fill(localMins, Long.MAX_VALUE);
        Arrays.fill(localMaxes, Long.MIN_VALUE);
        for (IntValueLookup[] stats : sessionStats) {
            for (int j = 0; j < numStats; j++) {
                localMins[j] = Math.min(localMins[j], stats[j].getMin());
                localMaxes[j] = Math.max(localMaxes[j], stats[j].getMax());
            }
        }
        for (int i = 0; i < numStats; i++) {
            final long range = localMaxes[i] - localMins[i];
            bits[i] = range == 0 ? 1 : 64-Long.numberOfLeadingZeros(range);
            if (bits[i] == 1 && booleanMetrics.size() < 4) {
                booleanMetrics.add(i);
            } else {
                longMetrics.add(i);
            }
        }
        mins = new long[numStats];
        maxes = new long[numStats];
        metrics = new int[numStats];
        for (int i = 0; i < booleanMetrics.size(); i++) {
            final int metric = booleanMetrics.getInt(i);
            mins[i] = localMins[metric];
            maxes[i] = localMaxes[metric];
            metrics[i] = metric;
        }
        // do exhaustive search for up to 10 metrics
        // optimizes first for least number of vectors then least space used for group stats
        // this is impractical beyond 10 due to being O(N!)
        if (longMetrics.size() <= 10) {
            final Permutation bestPermutation = permutations(longMetrics.toIntArray(), new ReduceFunction<int[], Permutation>() {
                @Override
                public Permutation apply(int[] ints, Permutation best) {
                    final Permutation permutation = getPermutation(ints, bits);
                    if (best == null) {
                        permutation.order = Arrays.copyOf(ints, ints.length);
                        return permutation;
                    }
                    if (permutation.vectorsUsed < best.vectorsUsed ||
                            (permutation.vectorsUsed == best.vectorsUsed && permutation.statsSpace < best.statsSpace)) {
                        // kinda strange for this to be mutable but only copy if better
                        permutation.order = Arrays.copyOf(ints, ints.length);
                        return permutation;
                    }
                    return best;
                }
            }, null);
            for (int i = 0, metricIndex = booleanMetrics.size(); i < bestPermutation.order.length; i++, metricIndex++) {
                metrics[metricIndex] = bestPermutation.order[i];
            }
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
            final ArrayList<IntArrayList> vectorMetrics = new ArrayList<IntArrayList>();
            spaceRemaining.add(12);
            vectorMetrics.add(new IntArrayList());
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
                    vectorMetrics.add(list);
                } else {
                    spaceRemaining.set(bestIndex, bestRemaining-size);
                    vectorMetrics.get(bestIndex).add(metric);
                }
            }
            int metricIndex = booleanMetrics.size();
            for (final IntArrayList list : vectorMetrics) {
                for (int j = 0; j < list.size(); j++) {
                    metrics[metricIndex++] = list.getInt(j);
                }
            }
        }
        for (int i = booleanMetrics.size(); i < metrics.length; i++) {
            mins[i] = localMins[metrics[i]];
            maxes[i] = localMaxes[metrics[i]];
        }
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

        final MultiCacher multiCacher = new MultiCacher(1);
        multiCacher.createMultiCache(0, null, new IntValueLookup[]{new DummyIntValueLookup(0, 1), new DummyIntValueLookup(0, 255), new DummyIntValueLookup(0, 1), new DummyIntValueLookup(0, 65535), new DummyIntValueLookup(0, 1), new DummyIntValueLookup(0, Long.MAX_VALUE), new DummyIntValueLookup(0, 1), new DummyIntValueLookup(0, 1000000), new DummyIntValueLookup(0, 1), new DummyIntValueLookup(0, Long.MAX_VALUE), new DummyIntValueLookup(0, Long.MAX_VALUE), new DummyIntValueLookup(0, Integer.MAX_VALUE*65536L), new DummyIntValueLookup(0, Integer.MAX_VALUE*65536L), new DummyIntValueLookup(0, Integer.MAX_VALUE*65536L), new DummyIntValueLookup(0, Integer.MAX_VALUE*65536L), new DummyIntValueLookup(0, Integer.MAX_VALUE*65536L), new DummyIntValueLookup(0, Integer.MAX_VALUE*65536L), new DummyIntValueLookup(0, Integer.MAX_VALUE*65536L)}, 18);
        System.out.println(Arrays.toString(multiCacher.metrics));
        System.out.println(Arrays.toString(multiCacher.mins));
        System.out.println(Arrays.toString(multiCacher.maxes));
    }

    private static final class Permutation {
        int[] order;
        final int vectorsUsed;
        final int statsSpace;

        private Permutation(int vectorsUsed, int statsSpace) {
            this.vectorsUsed = vectorsUsed;
            this.statsSpace = statsSpace;
        }
    }

    private Permutation getPermutation(int[] permutation, int[] bits) {
        int vectors = 1;
        int currentVectorStats = 0;
        int index = 4;
        int outputStats = 0;
        for (int metric : permutation) {
            final int metricSize = (bits[metric] + 7) / 8;
            if (index + metricSize > 16 * vectors) {
                outputStats += (currentVectorStats + 1) / 2 * 2;
                currentVectorStats = 0;
                index = 16 * vectors;
                vectors++;
            }
            index += metricSize;
            currentVectorStats++;
        }
        outputStats += (currentVectorStats+1)/2*2;
        return new Permutation(vectors, outputStats);
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

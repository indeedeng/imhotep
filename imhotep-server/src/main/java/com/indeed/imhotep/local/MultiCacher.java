package com.indeed.imhotep.local;

import com.google.common.base.Throwables;
import com.google.common.collect.Collections2;
import com.google.common.primitives.Ints;
import com.indeed.flamdex.api.IntValueLookup;
import com.indeed.util.core.sort.Quicksortable;
import com.indeed.util.core.sort.Quicksortables;
import it.unimi.dsi.fastutil.ints.IntArrayFIFOQueue;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import org.apache.log4j.Logger;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;

/**
 * @author jplaisance
 */
public final class MultiCacher {
    private static final Logger log = Logger.getLogger(MultiCacher.class);
    private final CyclicBarrier barrier;
    private byte[] statOrder;
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
        return null;
    }

    private void calculateMetricOrder() {
        final IntArrayList booleanMetrics = new IntArrayList();
        final IntArrayList metrics = new IntArrayList();
        mins = new long[numStats];
        maxes = new long[numStats];
        final int[] bits = new int[numStats];
        Arrays.fill(mins, Long.MAX_VALUE);
        Arrays.fill(maxes, Long.MIN_VALUE);
        for (int i = 0; i < sessionStats.length; i++) {
            for (int j = 0; j < numStats; j++) {
                mins[j] = Math.min(mins[j], sessionStats[i][j].getMin());
                maxes[j] = Math.max(maxes[j], sessionStats[i][j].getMax());
            }
        }
        for (int i = 0; i < numStats; i++) {
            final long range = maxes[i] - mins[i];
            bits[i] = range == 0 ? 1 : 64-Long.numberOfLeadingZeros(range);
            if (bits[i] == 1 && booleanMetrics.size() < 4) {
                booleanMetrics.add(i);
            } else {
                metrics.add(i);
            }
        }

        if (metrics.size() <= 10) {
            final Permutation bestPermutation = permutations(metrics.toIntArray(), new ReduceFunction<int[], Permutation>() {
                @Override
                public Permutation apply(int[] ints, Permutation best) {
                    final Permutation permutation = getPermutation(ints, bits, false);
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
            System.out.println("bestPermutation.vectorsUsed = " + bestPermutation.vectorsUsed);
            System.out.println("bestPermutation.statsSpace = " + bestPermutation.statsSpace);
            System.out.println("bestPermutation.order = " + Arrays.toString(bestPermutation.order));
            getPermutation(bestPermutation.order, bits, true);
            this.metrics = new int[numStats];
            int metricIndex = 0;
            for (int i = 0; i < booleanMetrics.size(); i++) {
                this.metrics[metricIndex++] = booleanMetrics.getInt(i);
            }
            for (int i = 0; i < bestPermutation.order.length; i++) {
                this.metrics[metricIndex++] = bestPermutation.order[i];
            }
        } else {
            Quicksortables.sort(new Quicksortable() {
                @Override
                public void swap(int i, int j) {
                    final int tmp = metrics.getInt(i);
                    metrics.set(i, metrics.getInt(j));
                    metrics.set(j, tmp);
                }

                @Override
                public int compare(int i, int j) {
                    return -Ints.compare(bits[metrics.getInt(i)], bits[metrics.getInt(j)]);
                }
            }, metrics.size());
            // todo sorted best fit
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
        time = -System.nanoTime();
        final Collection<List<Integer>> permutations = Collections2.permutations(new IntArrayList(ints));
        for (List<Integer> list : permutations);
        time += System.nanoTime();
        System.out.println(time / 1000000d);


        final MultiCacher multiCacher = new MultiCacher(1);
        multiCacher.createMultiCache(0, null, new IntValueLookup[]{new DummyIntValueLookup(0, 1), new DummyIntValueLookup(0, 255), new DummyIntValueLookup(0, 1), new DummyIntValueLookup(0, 65535), new DummyIntValueLookup(0, 1), new DummyIntValueLookup(0, Long.MAX_VALUE), new DummyIntValueLookup(0, 1), new DummyIntValueLookup(0, 1000000), new DummyIntValueLookup(0, 1), new DummyIntValueLookup(0, Long.MAX_VALUE), new DummyIntValueLookup(0, Long.MAX_VALUE), new DummyIntValueLookup(0, Integer.MAX_VALUE*65536L)}, 12);
        System.out.println(Arrays.toString(multiCacher.metrics));
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

    private Permutation getPermutation(int[] permutation, int[] bits, boolean print) {
        String str = "";
        int vectors = 1;
        int currentVectorStats = 0;
        int index = 4;
        int outputStats = 0;
        for (int i = 0; i < permutation.length; i++) {
            final int metricSize = (bits[permutation[i]]+7)/8;
            if (index+metricSize > 16*vectors) {
                outputStats += (currentVectorStats+1)/2*2;
                currentVectorStats = 0;
                if (16*vectors-index > 0 && print) str += "pad "+(16*vectors-index)+", ";
                index = 16*vectors;
                vectors++;
            }
            index += metricSize;
            if (print) str += metricSize+", ";
            currentVectorStats++;
        }
        outputStats += (currentVectorStats+1)/2*2;
        if (print) System.out.println(str.substring(0, str.length()-2));
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

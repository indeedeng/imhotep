package com.indeed.imhotep.shardmaster;

import com.indeed.imhotep.shardmaster.utils.IntervalTree;
import javafx.util.Pair;
import org.junit.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.*;

/**
 * @author kornerup
 */

public class IntervalTreeTest {
    @Test
    public void testSimpleAddAndQuery(){
        IntervalTree<Integer, Integer> tree = new IntervalTree<>();
        tree.addInterval(0, 10, 1);
        tree.addInterval(10, 15, 2);
        tree.addInterval(16, 20, 3);
        Assert.assertEquals(2,tree.getValuesInRange(0, 15).size());
        Assert.assertEquals(1,tree.getValuesInRange(0, 9).size());
        Assert.assertEquals(0,tree.getValuesInRange(-1, -1).size());
        Assert.assertEquals(1 ,tree.getValuesInRange(2, 3).size());
        Assert.assertTrue(tree.getValuesInRange(-1, 3).contains(1));
    }

    @Test
    public void randomizedAddAndQuery(){
        IntervalTree<Double, Integer> tree = new IntervalTree<>();
        List<Pair<Double,Double>> intervals = new ArrayList<>();
        for(int count = 0; count < 3000; count++) {
            double a = Math.random();
            double b = Math.random();
            double small = Math.min(a,b);
            double big = Math.max(a,b);
            intervals.add(new Pair<>(small, big));
            tree.addInterval(a, b, count);
        }
        for(int index = 0; index < intervals.size(); index++) {
            Pair<Double, Double> interval = intervals.get(index);
            Assert.assertTrue(tree.getValuesInRange(interval.getKey(), interval.getValue()).contains(index));
        }

        for(int count = 0; count < 1000; count ++) {
            double a = Math.random();
            double b = Math.random();
            double small = Math.min(a,b);
            double big = Math.max(a,b);
            Set<Integer> indexes = tree.getValuesInRange(small, big);
            for(int index: indexes) {
                Pair<Double, Double> interval = intervals.get(index);
                Assert.assertTrue(interval.getKey() < big && interval.getValue() > small);
            }
        }
    }

    //TODO: Find why this test passes when I don't have locks and make it break in that case
    @Test
    public void testConcurrency() throws InterruptedException, ExecutionException {
        IntervalTree<Double, Integer> tree = new IntervalTree<>();
        List<Pair<Double,Double>> intervals = new ArrayList<>();
        ExecutorService executorService = Executors.newFixedThreadPool(10);
        for(int count = 0; count < 3000; count++) {
            final int index = count;
            double a = Math.random();
            double b = Math.random();
            double small = Math.min(a,b);
            double big = Math.max(a,b);
            intervals.add(new Pair<>(small, big));
            executorService.submit(()->tree.addInterval(a, b, index));
        }
        for(int index = 0; index < intervals.size(); index++) {
            final int thisIndex = index;
            Pair<Double, Double> interval = intervals.get(index);
            executorService.submit(()->Assert.assertTrue(tree.getValuesInRange(interval.getKey(), interval.getValue()).contains(thisIndex)));
        }

        for(int count = 0; count < 1000; count ++) {
            double a = Math.random();
            double b = Math.random();
            double small = Math.min(a,b);
            double big = Math.max(a,b);
            executorService.submit(()->{
                Set<Integer> indexes = tree.getValuesInRange(small, big);
                for(int index: indexes) {
                    Pair<Double, Double> interval = intervals.get(index);
                    Assert.assertTrue(interval.getKey() < big && interval.getValue() > small);
                }
            });
        }

        executorService = Executors.newFixedThreadPool(10);

        final IntervalTree<Integer, Integer> tree2 = new IntervalTree<>();
        executorService.submit(() -> tree2.addInterval(0, 1, 0));
        executorService.submit(() -> tree2.addInterval(1, 2, 1));
        executorService.submit(() -> tree2.addInterval(2, 3, 2));
        executorService.submit(() -> tree2.addInterval(0, 3, 3));
        executorService.awaitTermination(1, TimeUnit.SECONDS);
        executorService.submit(() -> tree2.addInterval(-2, -1, 0));
        Future<Set<Integer>> future = executorService.submit(() -> tree2.getValuesInRange(0, 3));
        executorService.submit(() -> tree2.addInterval(4, 6, 2));
        executorService.awaitTermination(1, TimeUnit.SECONDS);
        Set<Integer> asynch = future.get();
        Set<Integer> synch = tree2.getValuesInRange(0,3);
        Assert.assertEquals(synch.size(), asynch.size());
        for(Integer i: asynch) {
            Assert.assertTrue(synch.contains(i));
        }
    }

    @Test
    public void testSearchBeforeAdd(){
        IntervalTree<Integer, Integer> tree = new IntervalTree<>();
        Assert.assertEquals(0, tree.getValuesInRange(0,1).size());
    }
}

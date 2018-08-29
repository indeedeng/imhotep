package com.indeed.imhotep.shardmaster;

import com.indeed.imhotep.shardmaster.utils.IntervalTree;
import javafx.util.Pair;
import org.junit.*;

import java.util.*;

/**
 * @author kornerup
 */

public class IntervalTreeTest {
    private static final double EPSILON = Double.MIN_NORMAL;

    @Test
    public void testBoundries() {
        final IntervalTree<Integer, Integer> tree = new IntervalTree<>();
        tree.addInterval(0,2, 0);
        tree.addInterval(2,4, 1);
        tree.addInterval(4,6, 2);
        Assert.assertTrue(tree.getValuesInRange(-1, 0).isEmpty());
        Assert.assertTrue(tree.getValuesInRange(0, 2).size() == 1);
        Assert.assertTrue(tree.getValuesInRange(6, 7).isEmpty());
    }

    @Test
    public void testSimpleAddAndQuery(){
        final IntervalTree<Integer, Integer> tree = new IntervalTree<>();
        tree.addInterval(0, 10, 1);
        tree.addInterval(10, 15, 2);
        tree.addInterval(16, 20, 3);
        Assert.assertEquals(2,tree.getValuesInRange(0, 15).size());
        Assert.assertEquals(1,tree.getValuesInRange(0, 9).size());
        Assert.assertEquals(0,tree.getValuesInRange(-1, -1).size());
        Assert.assertEquals(1 ,tree.getValuesInRange(2, 3).size());
        Assert.assertEquals(1 ,tree.getValuesInRange(10, 15).size());
        Assert.assertTrue(tree.getValuesInRange(-1, 3).contains(1));
    }

    @Test
    public void randomizedAddAndQuery(){
        final IntervalTree<Double, Integer> tree = new IntervalTree<>();
        final List<Pair<Double,Double>> intervals = new ArrayList<>();
        for(int count = 0; count < 10000; count++) {
            final double start = Math.random();
            final double end = start + (Math.random() / 1000) + EPSILON;
            intervals.add(new Pair<>(start, end));
            tree.addInterval(start, end, count);
        }
        System.out.println("done generating");
        long time = -System.currentTimeMillis();
        for(int index = 0; index < intervals.size(); index++) {
            final Pair<Double, Double> interval = intervals.get(index);
            Assert.assertTrue(tree.getValuesInRange(interval.getKey(), interval.getValue()+ EPSILON).contains(index));
        }
        time+=System.currentTimeMillis();
        System.out.println(time);

        for(int count = 0; count < 1000; count ++) {
            final double a = Math.random();
            final double b = Math.random();
            final double small = Math.min(a,b);
            final double big = Math.max(a,b) + EPSILON;
            final Set<Integer> indexes = tree.getValuesInRange(small, big);
            for(final int index: indexes) {
                final Pair<Double, Double> interval = intervals.get(index);
                Assert.assertTrue((interval.getKey() <= big) && (interval.getValue() > small));
            }
        }
    }

    @Test
    public void testSearchBeforeAdd(){
        final IntervalTree<Integer, Integer> tree = new IntervalTree<>();
        Assert.assertEquals(0, tree.getValuesInRange(0,1).size());
    }

    @Test
    public void testSimpleDelete() {
        final IntervalTree<Integer, Integer> tree = new IntervalTree<>();
        tree.addInterval(0, 10, 1);
        tree.addInterval(10, 15, 2);
        tree.addInterval(16, 20, 3);
        Assert.assertFalse(tree.deleteInterval(10,15,1));
        Assert.assertEquals(2,tree.getValuesInRange(0, 15).size());
        Assert.assertTrue(tree.deleteInterval(10,15,2));
        Assert.assertEquals(1,tree.getValuesInRange(0, 15).size());
        Assert.assertEquals(1,tree.getValuesInRange(0, 9).size());
        Assert.assertEquals(0,tree.getValuesInRange(-1, -1).size());
        Assert.assertEquals(1 ,tree.getValuesInRange(2, 3).size());
        Assert.assertEquals(0 ,tree.getValuesInRange(10, 15).size());
        Assert.assertTrue(tree.getValuesInRange(-1, 3).contains(1));
    }

    @Test
    public void randomDeleteTest() {
        final IntervalTree<Double, Integer> tree = new IntervalTree<>();
        final List<Pair<Double,Double>> intervals = new ArrayList<>();
        final List<Integer> values = new ArrayList<>();
        for(int count = 0; count < 10000; count++) {
            final double start = Math.random();
            final double end = start + (Math.random() / 1000) + EPSILON;
            intervals.add(new Pair<>(start, end));
            tree.addInterval(start, end, count);
            values.add(count);
        }
        System.out.println("done generating");
        System.out.println("now deleting");
        for(int count = 0; count < 1000; count++) {
            final int randIndex = new Random().nextInt(intervals.size());
            final Pair<Double, Double> interval = intervals.remove(randIndex);
            tree.deleteInterval(interval.getKey(), interval.getValue(), values.get(randIndex));
            values.remove(randIndex);
        }
        long time = -System.currentTimeMillis();
        for(int index = 0; index < intervals.size(); index++) {
            final Pair<Double, Double> interval = intervals.get(index);
            Assert.assertTrue(tree.getValuesInRange(interval.getKey(), interval.getValue()+ EPSILON).contains(values.get(index)));
        }
        time+=System.currentTimeMillis();
        System.out.println(time);

        for(int count = 0; count < 1000; count ++) {
            final double a = Math.random();
            final double b = Math.random();
            final double small = Math.min(a,b);
            final double big = Math.max(a,b) + EPSILON;
            final Set<Integer> indexes = tree.getValuesInRange(small, big);
            for(final int index: indexes) {
                final Pair<Double, Double> interval = intervals.get(Collections.binarySearch(values, index));
                Assert.assertTrue((interval.getKey() <= big) && (interval.getValue() > small));
            }
        }

        Assert.assertFalse(tree.hasEmptyIntervals());
    }
}

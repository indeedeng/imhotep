package com.indeed.imhotep.utils;

import com.google.common.collect.ImmutableList;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class BoundedPriorityQueueTest {

    @Test
    public void testOffer() {
        final List<Integer> inputList = ImmutableList.of(10, 15, 23, 13, 4, 12 ,-1, 14, 17, 1, 2, 12, 6, 3, 8, 10);
        final List<Integer> expected = ImmutableList.of(10, 12, 12, 13, 14, 15, 17, 23);

        final BoundedPriorityQueue<Integer> pq = new BoundedPriorityQueue<>(Integer::compareTo, 8);
        for (final Integer i : inputList) {
            pq.offer(i);
        }

        List<Integer> output = new ArrayList<>(10);
        while (!pq.isEmpty()) {
            output.add(pq.poll());
        }

        assertEquals(expected, output);
    }

    @Test
    public void testPeek() {
        final BoundedPriorityQueue<Integer> pq = new BoundedPriorityQueue<>(Integer::compareTo, 8);
        final List<Integer> inputList = ImmutableList.of(10, 15, 23, 13, 4, 12 ,-1, 14, 17, 1, 2, 12, 6, 3, 8, 10);
        final List<Integer> expected = ImmutableList.of(10, 10, 10, 10, 4, 4, -1, -1, 4, 4, 4, 10, 10, 10, 10, 10);

        final List<Integer> out = new ArrayList<>();
        for (final Integer i : inputList) {
            pq.offer(i);
            out.add(pq.peek());
        }
        assertEquals(expected, out);
    }

    @Test
    public void testSize() {
        final BoundedPriorityQueue<Integer> pq = new BoundedPriorityQueue<>(Integer::compareTo, 8);

        pq.offer(1);
        pq.offer(2);
        pq.offer(3);
        assertEquals(pq.size(), 3);

        pq.offer(4);
        pq.offer(5);
        pq.offer(6);
        pq.offer(7);
        pq.offer(8);
        pq.offer(9);
        assertEquals(pq.size(), 8);

        pq.poll();
        assertEquals(pq.size(), 7);
    }

    @Test
    public void testIterator() {
        final List<Integer> inputList = ImmutableList.of(10, 15, 23, 13, 4, 12 ,-1, 14, 17, 1, 2, 12, 6, 3, 8, 10);
        final List<Integer> expected = ImmutableList.of(10, 12, 12, 13, 14, 15, 17, 23);

        final BoundedPriorityQueue<Integer> pq = new BoundedPriorityQueue<>(Integer::compareTo, 8);
        for (final Integer i : inputList) {
            pq.offer(i);
        }
        Iterator<Integer> it = pq.iterator();

        List<Integer> output = new ArrayList<>(8);
        while (it.hasNext()) {
            output.add(it.next());
        }

        assertEquals(expected, output);
    }

    @Test
    public void testOfferReverse() {
        final List<Integer> inputList = ImmutableList.of(10, 15, 23, 13, 4, 12 ,-1, 14, 17, 1, 2, 12, 6, 3, 8, 10);
        final List<Integer> expected = ImmutableList.of(10, 8, 6, 4, 3, 2, 1, -1);

        final BoundedPriorityQueue<Integer> pq = new BoundedPriorityQueue<>(Comparator.reverseOrder(), 8);
        for (final Integer i : inputList) {
            pq.offer(i);
        }

        List<Integer> output = new ArrayList<>(10);
        while (!pq.isEmpty()) {
            output.add(pq.poll());
        }

        assertEquals(expected, output);
    }
}
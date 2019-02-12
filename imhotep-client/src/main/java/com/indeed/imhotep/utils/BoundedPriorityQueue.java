package com.indeed.imhotep.utils;

import it.unimi.dsi.fastutil.objects.ObjectHeapIndirectPriorityQueue;

import java.util.AbstractQueue;
import java.util.Comparator;
import java.util.Iterator;
import java.util.stream.IntStream;

/**
 * A priority queue with bounded capacity optimizes the offer method. In the case that queue is full,
 * new offered element will be replaced with peek element and notify the queue to adjust itself, rather
 * than poll + offer with two operations.
 * @param <E>
 */
public class BoundedPriorityQueue<E> extends AbstractQueue<E> {
    private final ObjectHeapIndirectPriorityQueue<E> pq;
    private final Comparator<? super E> comparator;
    private final int maxCapacity;

    // the array to store all element references
    private E[] elementRefArray;

    // the array to store all avaliable locations in
    private final int[] availableSlots;
    private int slotIndex;

    public BoundedPriorityQueue(final Comparator<? super E> comparator, final int maxCapacity) {
        this.maxCapacity = maxCapacity;
        this.comparator = comparator;

        elementRefArray = (E [])new Object[maxCapacity];
        pq = new ObjectHeapIndirectPriorityQueue<E>(elementRefArray, comparator);

        availableSlots = IntStream.range(0, maxCapacity).toArray();
        slotIndex = maxCapacity;
    }

    @Override
    public Iterator<E> iterator() {
        return new Itr();
    }

    @Override
    public boolean offer(final E e) {
        if (pq.size() < maxCapacity) {
            final int emptySlot = availableSlots[--slotIndex];
            elementRefArray[emptySlot] = e;
            pq.enqueue(emptySlot);
            return true;
        }

        if (comparator.compare(e, peek()) >= 0) {
            final int first = pq.first();
            // replace the old to new and adjust the heap
            elementRefArray[first] = e;
            pq.changed();
        }
        return true;
    }

    @Override
    public E poll() {
        final E e = peek();
        final int first = pq.dequeue();
        availableSlots[slotIndex++] = first;
        return e;
    }

    @Override
    public E peek() {
        final int first = pq.first();
        return elementRefArray[first];
    }

    @Override
    public int size() {
        return pq.size();
    }

    private final class Itr implements Iterator<E> {
        @Override
        public boolean hasNext() {
            return size() > 0;
        }

        @Override
        public E next() {
            return poll();
        }
    }
}

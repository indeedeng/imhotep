package com.indeed.imhotep.utils;

import it.unimi.dsi.fastutil.objects.ObjectArrays;
import it.unimi.dsi.fastutil.objects.ObjectHeaps;

import java.util.AbstractQueue;
import java.util.Comparator;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * A priority queue with bounded capacity optimizes the offer method. In the case that queue is full,
 * new offered element will be replaced with peek element and notify the queue to adjust itself, rather
 * than poll + offer with two operations.
 * @param <E>
 */
public class BoundedPriorityQueue<E> extends AbstractQueue<E> {
    private E[] heap;
    private int size;

    private final int maxCapacity;
    private final Comparator<? super E> comparator;

    public BoundedPriorityQueue(final int maxCapacity, final Comparator<? super E> comparator) {
        this.maxCapacity = maxCapacity;
        this.comparator = comparator;

        heap = (E[]) new Object[8];
        size = 0;
    }

    @Override
    public boolean offer(final E e) {
        if (size < maxCapacity) {
            internalOffer(e);
            return true;
        }
        if (comparator.compare(e, peek()) >= 0) {
            heap[0] = e;
            ObjectHeaps.downHeap(heap, size, 0, comparator);
            return true;
        }
        return false;
    }

    private void internalOffer(final E e) {
        if (size == heap.length) {
            heap = ObjectArrays.grow(heap, size+1);
        }

        heap[size++] = e;
        ObjectHeaps.upHeap(heap, size, size-1, comparator);
    }

    /**
     * Get all sorted elements and store in the dest array starting from startIndex
     */
    public void getTopK(final E[] dest, final int startIndex) {
        System.arraycopy(heap, 0, dest, startIndex, size);
        ObjectArrays.quickSort(dest, startIndex, startIndex + size, comparator);
    }

    @Override
    public E poll() {
        if (size == 0) {
            throw new NoSuchElementException();
        }

        final E e = heap[0];
        heap[0] = heap[--size];
        heap[size] = null;
        if (size != 0) {
            ObjectHeaps.downHeap(heap, size, 0, comparator);
        }
        return e;
    }

    @Override
    public E peek() {
        if (size == 0) {
            throw new NoSuchElementException();
        }
        return heap[0];
    }

    @Override
    public Iterator<E> iterator() {
        throw new UnsupportedOperationException("No implementation");
    }

    @Override
    public int size() {
        return size;
    }

    @Override
    public boolean isEmpty() {
        return size == 0;
    }
}
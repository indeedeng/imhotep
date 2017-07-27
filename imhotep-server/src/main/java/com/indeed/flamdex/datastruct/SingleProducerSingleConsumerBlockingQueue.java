package com.indeed.flamdex.datastruct;

import sun.misc.Unsafe;

import javax.annotation.Nonnull;
import java.lang.reflect.Field;
import java.util.Collection;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Efficient lock-free single producer single consumer queue. Locks are only used in these situations:
 * 1. adding an element to the queue using the blocking method put() and the queue is full.
 * 2. taking an element out using the take() method and the queue is empty.
 * This is to avoid busy waiting.
 * VERY IMPORTANT NOTE: The implementation does not work for multiple consumers or producers.
 * 1. padding to avoid false sharing
 * 2. lazy volatile writes (Unsafe.putOrderedInt()) to avoid expensive volatile writes
 * 3. tail and head caching to avoid expensive volatile reads
 *
 * @author arun
 */
public class SingleProducerSingleConsumerBlockingQueue<E> implements CopyingBlockingQueue<E> {

    // x86/sun 64 bit jdk alignment:
    // 16 bytes object header
    // 40 bytes padding
    // 8 bytes data
    // 64 bytes padding (to ensure header for next object which contains the futex
    // x86 currently uses 64 byte cache lines so it is impossible for the data to end up on the same cache line as anything else
    @SuppressWarnings("UnusedDeclaration")
    private abstract static class PrePadding {
        long pad1;
        long pad2;
        long pad3;
        long pad4;
        long pad5;
    }

    @SuppressWarnings("UnusedDeclaration")
    private abstract static class VolatileInt extends PrePadding {
        protected volatile int value;
        int pad;
    }

    @SuppressWarnings("UnusedDeclaration")
    private abstract static class Int extends PrePadding {
        int value;
        int pad;
    }

    @SuppressWarnings("UnusedDeclaration")
    private static final class PaddedVolatileInt extends VolatileInt {
        long pad1;
        long pad2;
        long pad3;
        long pad4;
        long pad5;
        long pad6;
        long pad7;
        long pad8;

        private static final Unsafe unsafe;
        private static final long valueOffset;

        static {
            try {
                final Field unsafeField = Unsafe.class.getDeclaredField("theUnsafe");
                unsafeField.setAccessible(true);
                unsafe = (Unsafe) unsafeField.get(null);
                valueOffset = unsafe.objectFieldOffset(VolatileInt.class.getDeclaredField("value"));
            } catch (final Exception ex) {
                throw new Error(ex);
            }
        }

        void lazySet(final int newValue) {
            unsafe.putOrderedInt(this, valueOffset, newValue);
        }

        void set(final int newValue) {
            value = newValue;
        }
    }

    @SuppressWarnings("UnusedDeclaration")
    static final class PaddedInt extends Int {
        long pad1;
        long pad2;
        long pad3;
        long pad4;
        long pad5;
        long pad6;
        long pad7;
        long pad8;
    }

    @SuppressWarnings("UnusedDeclaration")
    private abstract static class VolatileBool extends PrePadding {
        volatile boolean value;
        boolean pad1;
        boolean pad2;
        boolean pad3;
        boolean pad4;
        boolean pad5;
        boolean pad6;
        boolean pad7;
    }

    @SuppressWarnings("UnusedDeclaration")
    static final class PaddedVolatileBool extends VolatileBool {
        long pad1;
        long pad2;
        long pad3;
        long pad4;
        long pad5;
        long pad6;
        long pad7;
        long pad8;
    }

    private final int moduloMask;
    private final E[] buffer;
    private final ObjFactory<E> factory;
    private final ObjCopier<E> copier;
    private final E nil;

    private final PaddedVolatileInt tail = new PaddedVolatileInt();
    private final PaddedVolatileInt head = new PaddedVolatileInt();
    private final PaddedVolatileBool readerWaiting = new PaddedVolatileBool();
    private final PaddedVolatileBool writerWaiting = new PaddedVolatileBool();

    private final PaddedInt tailCache = new PaddedInt();
    private final PaddedInt headCache = new PaddedInt();

    private final Lock lock = new ReentrantLock();
    private final Condition notEmpty = lock.newCondition();
    private final Condition notFull = lock.newCondition();


    public SingleProducerSingleConsumerBlockingQueue(final int capacity) {
        if (capacity != Integer.highestOneBit(capacity)) {
            throw new RuntimeException("capacity must be a power of two!");
        }
        moduloMask = capacity-1;
        //noinspection unchecked
        buffer = (E[]) new Object[capacity];
        factory = null;
        copier = null;
        nil = null;
    }

    public SingleProducerSingleConsumerBlockingQueue(final int capacity,
                                                     final ObjFactory<E> factory,
                                                     final ObjCopier<E> copier) {
        if (capacity != Integer.highestOneBit(capacity)) {
            throw new RuntimeException("capacity must be a power of two!");
        }
        this.moduloMask = capacity - 1;
        //noinspection unchecked
        this.buffer = (E[]) new Object[capacity];
        this.factory = factory;
        this.copier = copier;
        this.nil = factory.getNil();

        for (int i = 0; i < capacity; i++) {
            this.buffer[i] = factory.newObj();
        }
    }

    @Override
    public boolean add(final E e) {
        if (offer(e)) {
            return true;
        }
        throw new IllegalStateException("Queue is full");
    }

    @Override
    public boolean offer(@Nonnull final E e) {
        if (null == e) {
            throw new NullPointerException("Can't write nulls!");
        }
        final int tailPtr = tail.value;
        final int cachedHead = getHeadFromCache(tailPtr);
        if (((tailPtr+1)&moduloMask) == cachedHead) {
            return false;
        } else {
            insert(e, tailPtr);
            return true;
        }
    }

    private void insert(final E e, final int tailPtr) {
        if (this.copier == null) {
            buffer[tailPtr] = e;
        } else {
            copier.copy(buffer[tailPtr], e);
        }
        tail.lazySet((tailPtr + 1) & moduloMask);
        notifyReader();
    }

    private void notifyReader() {
        if (readerWaiting.value) {
            lock.lock();
            try {
                if (head.value != tail.value) {
                    notEmpty.signal();
                    readerWaiting.value = false;
                }
            } finally {
                lock.unlock();
            }
        }
    }

    @Override
    public void put(final E e) throws InterruptedException {
        if (null == e) {
            throw new NullPointerException("Can't write nulls!");
        }
        final int tailPtr = tail.value;
        final int cachedHead = getHeadFromCache(tailPtr);
        if (((tailPtr+1)&moduloMask) == cachedHead) {
            waitForSpace();
        }
        insert(e, tailPtr);
    }

    private void waitForSpace() throws InterruptedException {
        lock.lockInterruptibly();
        try {
            writerWaiting.value = true;
            while (((tail.value+1)&moduloMask) == getHeadFromCache(tail.value)) {
                if (readerWaiting.value) {
                    notifyReader();
                } else {
                    notFull.await();
                }
            }
        } finally {
            writerWaiting.value = false;
            lock.unlock();
        }
    }

    private boolean waitForSpace(final long timeNanos) throws InterruptedException {
        lock.lockInterruptibly();
        try {
            long nanos = timeNanos;
            writerWaiting.value = true;
            while (((tail.value+1)&moduloMask) == getHeadFromCache(tail.value)) {
                if (nanos < 0){
                    break;
                }
                nanos = notFull.awaitNanos(nanos);
            }
            return ((tail.value+1)&moduloMask) != getHeadFromCache(tail.value);
        } finally {
            writerWaiting.value = false;
            lock.unlock();
        }
    }

    @Override
    public boolean offer(final E e, final long timeout, @Nonnull final TimeUnit unit) throws InterruptedException {
        if (e == null) {
            throw new NullPointerException("Can't accept nulls!");
        }
        final int tailPtr = tail.value;
        if (((tailPtr+1)&moduloMask) == getHeadFromCache(tailPtr)) {
            if (!waitForSpace(unit.toNanos(timeout))) {
                return false;
            }
        }
        insert(e, tailPtr);
        return true;
    }

    @Override
    public E take() throws InterruptedException {
        final int headPtr = head.value;
        final int cachedTail = getTailFromCache(headPtr);
        if (headPtr == cachedTail) {
            waitForInput();
        }
        return takeElement(headPtr);
    }

    public void take(final E dest) throws InterruptedException {
        final int headPtr = head.value;
        final int cachedTail = getTailFromCache(headPtr);
        if (headPtr == cachedTail) {
            waitForInput();
        }
        takeElementCopy(headPtr, dest);
    }

    private E takeElement(final int headPtr) {
        final E e;
        if (copier == null) {
            e = buffer[headPtr];
            head.lazySet((headPtr + 1) & moduloMask);
            notifyWriter();
        } else {
            e = factory.newObj();
            takeElementCopy(headPtr, e);
        }
        return e;
    }

    private void takeElementCopy(final int headPtr, final E ele) {
        final E e = buffer[headPtr];
        copier.copy(ele, e);
        head.lazySet((headPtr + 1) & moduloMask);
        notifyWriter();
    }

    private void waitForInput() throws InterruptedException {
        lock.lockInterruptibly();
        try {
            readerWaiting.value = true;
            final int headPtr = head.value;
            while (headPtr == getTailFromCache(headPtr)) {
                if (!writerWaiting.value) {
                    notEmpty.await();
                } else {
                    notifyWriter();
                }
            }
        } finally {
            readerWaiting.value = false;
            lock.unlock();
        }
    }

    private boolean waitForInput(final long timeoutNanos) throws InterruptedException {
        lock.lockInterruptibly();
        try {
            readerWaiting.value = true;
            long nanos = timeoutNanos;
            final int headPtr = head.value;
            while ((headPtr ==  getTailFromCache(headPtr))) {
                if (nanos < 0) {
                    return false;
                }
                nanos = notEmpty.awaitNanos(timeoutNanos);
            }
            return headPtr != getTailFromCache(headPtr);
        } finally {
            readerWaiting.value = false;
            lock.unlock();
        }
    }

    @Override
    public E poll(final long timeout, @Nonnull final TimeUnit unit) throws InterruptedException {
        final int headPtr = head.value;
        if (headPtr == getTailFromCache(headPtr)) {
            if (!waitForInput(unit.toNanos(timeout))) {
                return null;
            }
        }
        return takeElement(headPtr);
    }

    public void poll(final long timeout, final TimeUnit unit, final E dest) throws InterruptedException {
        final int headPtr = head.value;
        if (headPtr == getTailFromCache(headPtr)) {
            if (!waitForInput(unit.toNanos(timeout))) {
                copier.copy(dest, nil);
            }
        }
        takeElementCopy(headPtr, dest);
    }

    @Override
    public int remainingCapacity() {
        throw new UnsupportedOperationException("unsupported operation");
    }

    private int getHeadFromCache(final int tailPtr) {
        if (headCache.value == ((tailPtr+1)&moduloMask)) {
            headCache.value = head.value;
        }
        return headCache.value;
    }

    @Override
    public E poll() {
        final int headPtr = head.value;
        final int tail = getTailFromCache(headPtr);
        if (headPtr != tail) {
            return takeElement(headPtr);
        } else {
            return null;
        }
    }

    public void poll(final E dest) {
        final int headPtr = head.value;
        final int tail = getTailFromCache(headPtr);
        if (headPtr != tail) {
            takeElementCopy(headPtr, dest);
        } else {
            copier.copy(dest, nil);
        }
    }

    private int getTailFromCache(final int headPtr) {
        if (tailCache.value == headPtr) {
            tailCache.value = tail.value;
        }
        return tailCache.value;
    }

    private void notifyWriter() {
        if (writerWaiting.value) {
            lock.lock();
            try {
                if (head.value != ((tail.value+1)&moduloMask)) {
                    notFull.signal();
                    writerWaiting.value = false;
                }
            } finally {
                lock.unlock();
            }
        }
    }

    @Override
    public E remove() {
        final E e = poll();
        if (null == e) {
            throw new NoSuchElementException("Queue is empty");
        }
        return e;
    }

    public void removeCopy(final E dest) {
        poll(dest);
        if (factory.equalsNil(dest)) {
            throw new NoSuchElementException("Queue is empty");
        }
    }

    @Override
    public E element() {
        final E e = peek();
        if (null == e) {
            throw new NoSuchElementException("Queue is empty");
        }
        return e;
    }

    public void element(final E dest) {
        peek(dest);
        if (factory.equalsNil(dest)) {
            throw new NoSuchElementException("Queue is empty");
        }
    }

    @Override
    public E peek() {
        if (head.value == getTailFromCache(head.value)) {
            return null;
        }
        return buffer[head.value];
    }

    public void peek(final E dest) {
        if (head.value == getTailFromCache(head.value)) {
            copier.copy(dest, nil);
        }
        copier.copy(dest, buffer[head.value]);
    }

    @Override
    public int size() {
        throw new UnsupportedOperationException("unsupported operation");
    }

    @Override
    public boolean isEmpty() {
        throw new UnsupportedOperationException("unsupported operation");
    }

    @Override
    public boolean contains(final Object o) {
        throw new UnsupportedOperationException("unsupported operation");
    }

    @Override
    public int drainTo(@Nonnull final Collection<? super E> c) {
        int count = 0;
        while (true) {
            final E e = poll();
            if (e == null) {
                return count;
            }
            c.add(e);
            count++;
        }
    }

    @Override
    public int drainTo(@Nonnull final Collection<? super E> c, final int maxElements) {
        int count = 0;
        while (true) {
            final E e = poll();
            if (e == null || count == maxElements) {
                return count;
            }
            c.add(e);
            count++;
        }
    }

    @Nonnull
    @Override
    public Iterator<E> iterator() {
        throw new UnsupportedOperationException("unsupported operation");
    }

    @Nonnull
    @Override
    public Object[] toArray() {
        throw new UnsupportedOperationException("unsupported operation");
    }

    @Nonnull
    @Override
    public <T> T[] toArray(@Nonnull final T[] a) {
        throw new UnsupportedOperationException("unsupported operation");
    }

    @Override
    public boolean remove(final Object o) {
        throw new UnsupportedOperationException("unsupported operation");
    }

    @Override
    public boolean containsAll(@Nonnull final Collection<?> c) {
        throw new UnsupportedOperationException("unsupported operation");
    }

    @Override
    public boolean addAll(@Nonnull final Collection<? extends E> c) {
        for (final E e : c) {
            add(e);
        }
        return true;
    }

    @Override
    public boolean removeAll(@Nonnull final Collection<?> c) {
        throw new UnsupportedOperationException("unsupported operation");
    }

    @Override
    public boolean retainAll(@Nonnull final Collection<?> c) {
        throw new UnsupportedOperationException("unsupported operation");
    }

    @Override
    public void clear() {
        Object value;
        do {
            value = poll();
        } while (null != value);
    }
}
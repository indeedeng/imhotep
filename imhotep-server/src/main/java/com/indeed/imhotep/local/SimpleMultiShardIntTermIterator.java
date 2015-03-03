package com.indeed.imhotep.local;

import com.google.common.primitives.Longs;
import com.indeed.flamdex.simple.SimpleIntTermIterator;
import com.indeed.util.core.Pair;
import com.indeed.util.core.io.Closeables2;
import org.apache.log4j.Logger;

import java.io.Closeable;
import java.util.Arrays;
import java.util.Comparator;
import java.util.PriorityQueue;

/**
 * @author arun.
 */
public class SimpleMultiShardIntTermIterator implements MultiShardIntTermIterator, Closeable {
    private static final Logger log = Logger.getLogger(SimpleMultiShardIntTermIterator.class);
    private final SimpleIntTermIterator[] intTermIterators;
    private final PriorityQueue<Pair<SimpleIntTermIterator, Integer>> pq;
    private long term;
    private final long[] offsets;

    SimpleMultiShardIntTermIterator(SimpleIntTermIterator[] intTermIterators) {
        this.intTermIterators = Arrays.copyOf(intTermIterators, intTermIterators.length);
        this.pq = new PriorityQueue<>(intTermIterators.length, new Comparator<Pair<SimpleIntTermIterator, Integer>>() {
            @Override
            public int compare(final Pair<SimpleIntTermIterator, Integer> o1, final Pair<SimpleIntTermIterator, Integer> o2) {
                return Longs.compare(o1.getFirst().term(), o2.getFirst().term());
            }
        });
        for (int i = 0; i < intTermIterators.length; i++) {
            final SimpleIntTermIterator simpleIntTermIterator = intTermIterators[i];
            if (simpleIntTermIterator.next()) {
                pq.add(new Pair<>(simpleIntTermIterator, i));
            }
        }
        offsets = new long[intTermIterators.length];
    }

    @Override
    public long term() {
        return term;
    }

    @Override
    public boolean next() {
        if (pq.isEmpty()) {
            return false;
        }
        term = pq.peek().getFirst().term();
        Arrays.fill(offsets, -1);
        while (!pq.isEmpty()) {
            final Pair<SimpleIntTermIterator, Integer> top = pq.remove();
            if (top.getFirst().term() == term) {
                offsets[top.getSecond()] = top.getFirst().getOffset();
                if (top.getFirst().next()) {
                    pq.add(top);
                }
            } else {
                pq.add(top);
                break;
            }
        }
        return true;
    }

    @Override
    public void offsets(long[] buffer) {
        System.arraycopy(offsets, 0, buffer, 0, offsets.length);
    }

    @Override
    public void close() {
        for (final SimpleIntTermIterator simpleIntTermIterator : intTermIterators) {
            if (simpleIntTermIterator != null) {
                Closeables2.closeQuietly(simpleIntTermIterator, log);
            }
        }
    }
}

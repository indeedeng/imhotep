package com.indeed.flamdex.simple;

import com.indeed.util.core.Pair;
import com.indeed.util.core.io.Closeables2;
import org.apache.log4j.Logger;

import java.util.Arrays;
import java.util.Comparator;
import java.util.PriorityQueue;

/**
 * @author arun.
 */
class SimpleMultiShardStringTermIterator implements MultiShardStringTermIterator {
    private static final Logger log = Logger.getLogger(SimpleMultiShardIntTermIterator.class);
    final SimpleStringTermIterator[] stringTermIterators;
    private final PriorityQueue<Pair<SimpleStringTermIterator, Integer>> pq;
    private String term;
    private final long[] offsets;

    SimpleMultiShardStringTermIterator(final SimpleStringTermIterator[] stringTermIterators) {
        this.stringTermIterators = Arrays.copyOf(stringTermIterators, stringTermIterators.length);
        this.pq = new PriorityQueue<>(stringTermIterators.length, new Comparator<Pair<SimpleStringTermIterator, Integer>>() {
            @Override
            public int compare(final Pair<SimpleStringTermIterator, Integer> o1, final Pair<SimpleStringTermIterator, Integer> o2) {
                return o1.getFirst().term().compareTo(o2.getFirst().term());
            }
        });
        for (int i = 0; i < stringTermIterators.length; i++) {
            final SimpleStringTermIterator simpleIntTermIterator = stringTermIterators[i];
            if (simpleIntTermIterator.next()) {
                pq.add(new Pair<>(simpleIntTermIterator, i));
            }
        }
        offsets = new long[stringTermIterators.length];
    }

    @Override
    public String term() {
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
            final Pair<SimpleStringTermIterator, Integer> top = pq.remove();
            if (top.getFirst().term().equals(term)) {
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
        for (final SimpleStringTermIterator stringTermIterator : stringTermIterators) {
            if (stringTermIterator != null) {
                Closeables2.closeQuietly(stringTermIterator, log);
            }
        }
    }
}

package com.indeed.flamdex.dynamic;

import com.indeed.flamdex.api.IntTermIterator;
import com.indeed.util.core.io.Closeables2;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.longs.LongHeapSemiIndirectPriorityQueue;
import org.apache.log4j.Logger;

import javax.annotation.Nonnull;
import java.util.List;

/**
 * {@link IntTermIterator} that merges several {@link IntTermIterator}.
 * @author michihiko
 */
class MergedIntTermIterator extends MergedTermIterator implements IntTermIterator {
    private static final Logger LOG = Logger.getLogger(MergedIntTermIterator.class);

    private final List<IntTermIterator> intTermIterators;
    private final IntList currentMinimums; // indices which satisfies currentTerms[i] == currentTerm
    private long currentTerm;
    private int currentTermFreq;
    private final long[] currentTerms;
    private final LongHeapSemiIndirectPriorityQueue priorityQueue;

    MergedIntTermIterator(@Nonnull final List<IntTermIterator> intTermIterators) {
        this.intTermIterators = intTermIterators;
        this.currentMinimums = new IntArrayList(this.intTermIterators.size());
        this.currentTerms = new long[intTermIterators.size()];
        this.priorityQueue = new LongHeapSemiIndirectPriorityQueue(this.currentTerms);
        for (int i = 0; i < this.intTermIterators.size(); ++i) {
            currentMinimums.add(i);
        }
    }

    private void innerReset() {
        priorityQueue.clear();
        for (int i = 0; i < intTermIterators.size(); ++i) {
            final IntTermIterator iterator = intTermIterators.get(i);
            if (iterator.next()) {
                currentTerms[i] = iterator.term();
                priorityQueue.enqueue(i);
            }
        }
        prepareNext();
    }

    private void prepareNext() {
        currentTermFreq = 0;
        currentMinimums.clear();
        if (!priorityQueue.isEmpty()) {
            currentTerm = currentTerms[priorityQueue.first()];
            while (!priorityQueue.isEmpty() && (currentTerm == currentTerms[priorityQueue.first()])) {
                final int i = priorityQueue.dequeue();
                currentTermFreq += intTermIterators.get(i).docFreq();
                currentMinimums.add(i);
            }
        }
    }

    @Nonnull
    @Override
    IntTermIterator getInnerTermIterator(final int idx) {
        return intTermIterators.get(idx);
    }

    @Nonnull
    @Override
    IntList getCurrentMinimums(){
        return currentMinimums;
    }

    @Override
    public void reset(final long term) {
        for (final IntTermIterator iterator : intTermIterators) {
            iterator.reset(term);
        }
        innerReset();
    }

    @Override
    public long term() {
        return currentTerm;
    }

    @Override
    public boolean next() {
        if (currentMinimums.isEmpty()) {
            return false;
        }
        for (final int i : currentMinimums) {
            final IntTermIterator iterator = intTermIterators.get(i);
            if (iterator.next()) {
                currentTerms[i] = iterator.term();
                priorityQueue.enqueue(i);
            }
        }
        prepareNext();
        return !currentMinimums.isEmpty();
    }

    @Override
    public int docFreq() {
        return currentTermFreq;
    }

    @Override
    public void close() {
        Closeables2.closeAll(intTermIterators, LOG);
    }
}

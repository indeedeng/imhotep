package com.indeed.flamdex.dynamic;

import com.indeed.flamdex.api.StringTermIterator;
import com.indeed.util.core.io.Closeables2;
import it.unimi.dsi.fastutil.objects.ObjectHeapSemiIndirectPriorityQueue;
import org.apache.log4j.Logger;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;

/**
 * {@link StringTermIterator} that merges several {@link StringTermIterator}.
 * @author michihiko
 */
class MergedStringTermIterator extends MergedTermIterator implements StringTermIterator {
    private static final Logger LOG = Logger.getLogger(MergedStringTermIterator.class);

    private final List<StringTermIterator> stringTermIterators;
    private final List<Integer> currentMinimums; // indices which satisfies currentTerms[i] == currentTerm
    private String currentTerm;
    private int currentTermFreq;
    private final String[] currentTerms;
    private final ObjectHeapSemiIndirectPriorityQueue<String> priorityQueue;

    MergedStringTermIterator(@Nonnull final List<StringTermIterator> stringTermIterators) {
        this.stringTermIterators = stringTermIterators;
        this.currentMinimums = new ArrayList<>(this.stringTermIterators.size());
        this.currentTerms = new String[stringTermIterators.size()];
        this.priorityQueue = new ObjectHeapSemiIndirectPriorityQueue<>(this.currentTerms);
        for (int i = 0; i < this.stringTermIterators.size(); ++i) {
            currentMinimums.add(i);
        }
    }

    private void innerReset() {
        priorityQueue.clear();
        for (int i = 0; i < stringTermIterators.size(); ++i) {
            final StringTermIterator iterator = stringTermIterators.get(i);
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
            while (!priorityQueue.isEmpty() && (currentTerm.equals(currentTerms[priorityQueue.first()]))) {
                final int i = priorityQueue.dequeue();
                currentTermFreq += stringTermIterators.get(i).docFreq();
                currentMinimums.add(i);
            }
        }
    }

    @Nonnull
    @Override
    StringTermIterator getInnerTermIterator(final int idx) {
        return stringTermIterators.get(idx);
    }

    @Override
    public void reset(final String term) {
        for (final StringTermIterator iterator : stringTermIterators) {
            iterator.reset(term);
        }
        innerReset();
    }

    @Override
    public String term() {
        return currentTerm;
    }

    @Override
    public boolean next() {
        if (currentMinimums.isEmpty()) {
            return false;
        }
        for (final int i : currentMinimums) {
            final StringTermIterator iterator = stringTermIterators.get(i);
            if (iterator.next()) {
                currentTerms[i] = iterator.term();
                priorityQueue.enqueue(i);
            }
        }
        prepareNext();
        return true;
    }

    @Override
    public int docFreq() {
        return currentTermFreq;
    }

    @Override
    public void close() {
        Closeables2.closeAll(stringTermIterators, LOG);
    }
}

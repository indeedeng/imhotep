package com.indeed.flamdex.dynamic;

import com.indeed.flamdex.api.StringTermIterator;
import com.indeed.util.core.io.Closeables2;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.ObjectHeapSemiIndirectPriorityQueue;
import org.apache.log4j.Logger;

import javax.annotation.Nonnull;
import java.util.List;

/**
 * {@link StringTermIterator} that merges several {@link StringTermIterator}.
 *
 * @author michihiko
 */
class MergedStringTermIterator implements MergedTermIterator, StringTermIterator {
    private static final Logger LOG = Logger.getLogger(MergedStringTermIterator.class);

    private final List<StringTermIterator> stringTermIterators;
    // We have to store iterators which have 'currentTerm' as that status to be able to do MergedDocIdStream#reset(MergedTermIterator).
    // this is the indices which satisfies currentTerms[i] == currentTerm, which is removed from priority queue until the next call of next().
    private final IntList currentMinimums;
    private String currentTerm;
    private int currentTermFreq;
    private final String[] currentTerms;
    private final ObjectHeapSemiIndirectPriorityQueue<String> priorityQueue;

    MergedStringTermIterator(@Nonnull final List<StringTermIterator> stringTermIterators) {
        this.stringTermIterators = stringTermIterators;
        this.currentMinimums = new IntArrayList(this.stringTermIterators.size());
        this.currentTerms = new String[stringTermIterators.size()];
        this.priorityQueue = new ObjectHeapSemiIndirectPriorityQueue<>(this.currentTerms);
        // Until first call of next(), this iterator should be invalid.
        // This state can be consider as "we have sentinel (lexicographically less than all possible string) as the term before the first call of next(), and call next() to skip it".
        // So, initially, all iterators are in currentMinimums
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
    public StringTermIterator getInnerTermIterator(final int idx) {
        return stringTermIterators.get(idx);
    }

    @Nonnull
    @Override
    public IntList getCurrentMinimums() {
        return currentMinimums;
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
        for (final int i : currentMinimums) {
            final StringTermIterator iterator = stringTermIterators.get(i);
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
        Closeables2.closeAll(stringTermIterators, LOG);
    }
}

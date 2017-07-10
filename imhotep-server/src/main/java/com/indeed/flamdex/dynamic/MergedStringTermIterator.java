package com.indeed.flamdex.dynamic;

import com.indeed.flamdex.api.StringTermIterator;
import com.indeed.util.core.io.Closeables2;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntArrays;
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
    private final IntArrayList currentMinimums;
    private String currentTerm;
    private int currentTermFreq;
    private final String[] currentTerms;
    private final ObjectHeapSemiIndirectPriorityQueue<String> priorityQueue;

    MergedStringTermIterator(@Nonnull final List<StringTermIterator> stringTermIterators) {
        this.stringTermIterators = stringTermIterators;
        this.currentMinimums = new IntArrayList(this.stringTermIterators.size());
        this.currentTerms = new String[stringTermIterators.size()];
        this.priorityQueue = new ObjectHeapSemiIndirectPriorityQueue<>(this.currentTerms);
        innerReset();
    }

    /**
     * For given {@code stringTermIterators} which are invalid until next call of {@link StringTermIterator#next()},
     * reset current status to match those iterators.
     * This iterator is invalid until next call of {@link MergedStringTermIterator#next()}
     */
    private void innerReset() {
        priorityQueue.clear();
        // All iterators must be in currentMinimums since they're waiting for call of next().
        currentMinimums.clear();
        for (int i = 0; i < stringTermIterators.size(); ++i) {
            currentMinimums.add(i);
        }
        // We reset other states (current{Term, TermFreq, Terms}) in the next call of next().
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
        currentMinimums.clear();
        currentTermFreq = 0;
        if (!priorityQueue.isEmpty()) {
            currentTerm = currentTerms[priorityQueue.first()];
            while (!priorityQueue.isEmpty() && (currentTerm.equals(currentTerms[priorityQueue.first()]))) {
                final int i = priorityQueue.dequeue();
                currentTermFreq += stringTermIterators.get(i).docFreq();
                currentMinimums.add(i);
            }
        }
        IntArrays.quickSort(currentMinimums.elements(), 0, currentMinimums.size());
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

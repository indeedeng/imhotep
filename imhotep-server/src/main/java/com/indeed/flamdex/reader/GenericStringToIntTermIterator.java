/*
 * Copyright (C) 2018 Indeed Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.indeed.flamdex.reader;

import com.google.common.base.Supplier;
import com.indeed.flamdex.api.IntTermIterator;
import com.indeed.flamdex.api.StringTermIterator;
import com.indeed.flamdex.api.TermIterator;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.PriorityQueue;

/**
 * Allows iteration over string Flamdex fields in numeric order.
 * <p>
 * You may need to add some code to the concrete class of {@link com.indeed.flamdex.api.DocIdStream#reset(TermIterator)}.
 *
 * @author vladimir
 */
public class GenericStringToIntTermIterator<I extends StringTermIterator> implements IntTermIterator {
    protected final I stringTermIterator;
    protected final Supplier<I> stringTermIteratorSupplier;

    /**
     * @param stringTermIterator         initial iterator used for initialization
     * @param stringTermIteratorSupplier used to create new SimpleStringTermIterator instances to have multiple iteration cursors in parallel
     */
    public GenericStringToIntTermIterator(
            final I stringTermIterator,
            final Supplier<I> stringTermIteratorSupplier
    ) {
        this.stringTermIterator = stringTermIterator;
        this.stringTermIteratorSupplier = stringTermIteratorSupplier;
    }

    // the rest of the code was adapted from com/indeed/flamdex/lucene/LuceneIntTermIterator.java

    // only handles positive numbers
    private static final List<String> intPrefixes;

    static {
        final List<String> temp = new ArrayList<>();
        temp.add("0");
        // TODO: support negative values
        for (int i = 1; i <= 9; i++) {
            for (long m = 1; m > 0 && m * i > 0; m *= 10) {
                temp.add(String.valueOf(m * i));
            }
        }
        // should already be sorted but re-sorting in case I screwed up -ahudson
        Collections.sort(temp);
        intPrefixes = Collections.unmodifiableList(temp);
    }

    private static class Prefix<I extends StringTermIterator> implements Comparable<Prefix<I>> {
        private final String firstTerm;
        private final int length;
        private final char prefix;
        private final Supplier<I> stringTermIteratorSupplier;

        private I stringTermIterator;
        private boolean endOfStream = false;
        private long val;

        public Prefix(final Supplier<I> stringTermIteratorSupplier, final String firstTerm) {
            this.stringTermIteratorSupplier = stringTermIteratorSupplier;
            this.prefix = firstTerm.charAt(0);
            this.length = firstTerm.length();
            this.firstTerm = firstTerm;
        }

        private void reset() {
            closeStringTermIterator();
            endOfStream = false;
        }

        private void closeStringTermIterator() {
            if (stringTermIterator != null) {
                stringTermIterator.close();
            }
            stringTermIterator = null;
        }

        private void initialize() {
            stringTermIterator = stringTermIteratorSupplier.get();
            stringTermIterator.reset(firstTerm);
            if (!stringTermIterator.next() || !firstTerm.equals(stringTermIterator.term())) {
                throw new RuntimeException("Serious bug detected, term was " + stringTermIterator.term() + ", expected " + firstTerm);
            }
            val = Long.parseLong(firstTerm);
        }

        public boolean next() {
            if (endOfStream) {
                return false;
            }

            if (stringTermIterator == null) {
                initialize();
                return true; // guaranteed to always work
            }

            String nextTargetString = Long.toString(val + 1);
            while (true) {
                if (nextTargetString.length() != length || nextTargetString.charAt(0) != prefix) {
                    closeStringTermIterator();
                    endOfStream = true;
                    return false;
                }

                stringTermIterator.reset(nextTargetString);
                final boolean skipSuccess = stringTermIterator.next();

                if (!skipSuccess ||
                        stringTermIterator.term() == null ||
                        stringTermIterator.term().charAt(0) != prefix) {
                    closeStringTermIterator();
                    endOfStream = true;
                    return false;
                }

                final String currentTerm = stringTermIterator.term();
                if (currentTerm.length() == length) {
                    val = Long.parseLong(currentTerm);
                    // todo deal with potential parse int errors?
                    return true;
                }

                // length is either longer or shorter, either way find the next targetString w/ prefix and length
                if (currentTerm.length() > length) {
                    nextTargetString = Long.toString(Long.parseLong(currentTerm.substring(0, length)) + 1);
                } else {
                    final StringBuilder sb = new StringBuilder(length);
                    sb.append(currentTerm);
                    while (sb.length() != length) {
                        sb.append('0');
                    }
                    nextTargetString = sb.toString();
                }
            }
        }

        @Override
        public int compareTo(@Nonnull final Prefix<I> o) {
            if (val < o.val) {
                return -1;
            }
            if (val > o.val) {
                return 1;
            }
            throw new RuntimeException("Impossible condition occurred");
        }
    }

    private PriorityQueue<Prefix<I>> prefixQueue;
    private List<Prefix<I>> prefixes;
    private long firstTerm = 0;

    private List<Prefix<I>> determineAppropriatePrefixes() {
        final String[][] firstTerm = new String[19][10];
        stringTermIterator.reset("0");

        while (stringTermIterator.next()) {
            final String term = stringTermIterator.term();
            if (term == null || term.charAt(0) > '9') {
                break;
            }

            final int x = term.length() - 1;
            if (x >= 0 && x < firstTerm.length) {
                final int y = term.charAt(0) - '0';
                if (y >= 0 && y < firstTerm[x].length) {
                    if (firstTerm[x][y] == null) {
                        firstTerm[x][y] = term;
                    }
                }
            }
        }

        final List<Prefix<I>> ret = new ArrayList<>();
        for (final String intPrefix : intPrefixes) {
            final int x = intPrefix.length() - 1;
            final int y = intPrefix.charAt(0) - '0';
            if (firstTerm[x][y] != null) {
                ret.add(new Prefix<>(stringTermIteratorSupplier, firstTerm[x][y]));
                firstTerm[x][y] = null;
            }
        }

        return ret;
    }

    private void initialize(final long term) {
        if (prefixes == null) {
            prefixes = determineAppropriatePrefixes();
        } else {
            for (final Prefix prefix : prefixes) {
                prefix.reset();
            }
        }
        if (prefixes.isEmpty()) {
            prefixQueue = new PriorityQueue<>(1);
            return;
        }
        prefixQueue = new PriorityQueue<>(prefixes.size());
        for (final Prefix<I> prefix : prefixes) {
            if (prefix.next()) {
                prefixQueue.add(prefix);
            }
        }
        while (!prefixQueue.isEmpty()) {
            if (prefixQueue.element().val >= term) {
                break;
            }
            final Prefix<I> prefix = prefixQueue.remove();
            if (prefix.next()) {
                prefixQueue.add(prefix);
            }
        }
    }

    public I getCurrentStringTermIterator() {
        return prefixQueue.element().stringTermIterator;
    }

    @Override
    public boolean next() {
        if (prefixQueue == null) {
            initialize(firstTerm);
        } else if (!prefixQueue.isEmpty()) {
            final Prefix<I> prefix = prefixQueue.remove();
            if (prefix.next()) {
                prefixQueue.add(prefix);
            }
        }

        return !prefixQueue.isEmpty();
    }

    @Override
    public int docFreq() {
        sanityCheck();
        return getCurrentStringTermIterator().docFreq();
    }

    @Override
    public void close() {
        stringTermIterator.close();
        if (prefixes != null) {
            for (final Prefix prefix : prefixes) {
                prefix.closeStringTermIterator();
            }
        }
    }

    @Override
    public void reset(final long term) {
        firstTerm = term;
        prefixQueue = null;
    }

    @Override
    public long term() {
        sanityCheck();
        return prefixQueue.element().val;
    }

    protected void sanityCheck() {
        if (prefixQueue == null || prefixQueue.isEmpty()) {
            throw new IllegalArgumentException("Invalid operation given iterators current state");
        }
    }
}

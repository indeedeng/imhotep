package com.indeed.flamdex.simple;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.PriorityQueue;

/**
 * Allows iteration over string Flamdex fields in numeric order.
 * @author vladimir
 */

public class StringToIntTermIterator implements SimpleIntTermIterator  {
    private final SimpleStringTermIterator stringTermIterator;
    private final SimpleFlamdexReader simpleFlamdexReader;
    private final String field;
    private final String filename;

    /**
     * @param stringTermIterator initial iterator used for initialization
     * @param simpleFlamdexReader used to create new SimpleStringTermIterator instances to have multiple iteration cursors in parallel
     * @param field name of the field being iterated over
     */
    public StringToIntTermIterator(
            SimpleStringTermIterator stringTermIterator, SimpleFlamdexReader simpleFlamdexReader, String field) {
        this.stringTermIterator = stringTermIterator;
        this.simpleFlamdexReader = simpleFlamdexReader;
        this.field = field;
        this.filename = stringTermIterator.getFilename();
    }

    @Override
    public String getFilename() {
        return filename;
    }

    // the rest of the code was adapted from com/indeed/flamdex/lucene/LuceneIntTermIterator.java

    // only handles positive numbers
    private static final List<String> intPrefixes;

    static {
        final List<String> temp = new ArrayList<String>();
        temp.add("0");
        // TODO: support negative values
        for (int i = 1; i <= 9; i++) {
            for (long m = 1; m > 0 && m*i > 0; m *= 10) {
                temp.add(""+(m*i));
            }
        }
        // should already be sorted but re-sorting in case I screwed up -ahudson
        Collections.sort(temp);
        intPrefixes = Collections.unmodifiableList(temp);
    }

    static class Prefix implements Comparable<Prefix> {
        final SimpleFlamdexReader reader;
        final String field;
        final String firstTerm;
        final int length;
        final char prefix;

        SimpleStringTermIterator termEnum;
        boolean endOfStream = false;
        long val;

        public Prefix(final SimpleFlamdexReader reader, final String field, final String firstTerm) {
            this.reader = reader;
            this.field = field;
            this.prefix = firstTerm.charAt(0);
            this.length = firstTerm.length();
            this.firstTerm = firstTerm;
        }

        private void reset() {
            closeTermEnum();
            endOfStream = false;
        }

        private void closeTermEnum() {
            if (termEnum != null) {
                termEnum.close();
            }
            termEnum = null;
        }

        private void initialize() {
            termEnum = reader.getStringTermIterator(field);
            termEnum.reset(firstTerm);
            if (!termEnum.next() || !firstTerm.equals(termEnum.term())) {
                throw new RuntimeException("Serious bug detected, term was "+termEnum.term()+", expected "+firstTerm);
            }
            val = Long.parseLong(firstTerm);
        }

        public boolean next() {
            if (endOfStream) return false;

            if (termEnum == null) {
                initialize();
                return true; // guaranteed to always work
            }

            String nextTargetString = Long.toString(val+1);
            while (true) {
                if (nextTargetString.length() != length || nextTargetString.charAt(0) != prefix) {
                    closeTermEnum();
                    endOfStream = true;
                    return false;
                }

                termEnum.reset(nextTargetString);
                final boolean skipSuccess = termEnum.next();

                if (!skipSuccess ||
                        termEnum.term() == null ||
                        termEnum.term().charAt(0) != prefix) {
                    closeTermEnum();
                    endOfStream = true;
                    return false;
                }

                final String currentTerm = termEnum.term();
                if (currentTerm.length() == length) {
                    val = Long.parseLong(currentTerm);
                    // todo deal with potential parse int errors?
                    return true;
                }

                // length is either longer or shorter, either way find the next targetString w/ prefix and length
                if (currentTerm.length() > length) {
                    nextTargetString = Long.toString(Long.parseLong(currentTerm.substring(0, length))+1);
                } else {
                    final StringBuilder sb = new StringBuilder(length);
                    sb.append(currentTerm);
                    while (sb.length() != length) sb.append('0');
                    nextTargetString = sb.toString();
                }
            }
        }

        @Override
        public int compareTo(final Prefix o) {
            if (val < o.val) return -1;
            if (val > o.val) return 1;
            throw new RuntimeException("Impossible condition occurred");
        }
    }

    private PriorityQueue<Prefix> prefixQueue;
    private List<Prefix> prefixes;
    private long firstTerm = 0;

    private List<Prefix> determineAppropriatePrefixes() {
        final String[][] firstTerm = new String[19][10];
        stringTermIterator.reset("0");

        while (stringTermIterator.next()) {
            final String term = stringTermIterator.term();
            if (term == null || term.charAt(0) > '9') break;

            final int x = term.length()-1;
            if (x >= 0 && x < firstTerm.length) {
                final int y = term.charAt(0)-'0';
                if (y >= 0 && y < firstTerm[x].length) {
                    if (firstTerm[x][y] == null) {
                        firstTerm[x][y] = term;
                    }
                }
            }
        }

        stringTermIterator.close();

        final List<Prefix> ret = new ArrayList<Prefix>();
        for (final String intPrefix : intPrefixes) {
            final int x = intPrefix.length()-1;
            final int y = intPrefix.charAt(0)-'0';
            if (firstTerm[x][y] != null) {
                ret.add(new Prefix(simpleFlamdexReader, field, firstTerm[x][y]));
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
            prefixQueue = new PriorityQueue<Prefix>(1);
            return;
        }
        prefixQueue = new PriorityQueue<Prefix>(prefixes.size());
        for (final Prefix prefix : prefixes) {
            if (prefix.next()) {
                prefixQueue.add(prefix);
            }
        }
        while (!prefixQueue.isEmpty()) {
            if (prefixQueue.element().val >= term) break;
            final Prefix prefix = prefixQueue.remove();
            if (prefix.next()) {
                prefixQueue.add(prefix);
            }
        }
    }

    @Override
    public boolean next() {
        if (prefixQueue == null) {
            initialize(firstTerm);
        } else if (!prefixQueue.isEmpty()) {
            final Prefix prefix = prefixQueue.remove();
            if (prefix.next()) prefixQueue.add(prefix);
        }

        return !prefixQueue.isEmpty();
    }

    @Override
    public int docFreq() {
        sanityCheck();
        return prefixQueue.element().termEnum.docFreq();
    }

    @Override
    public void close() {
        stringTermIterator.close();
        if(prefixes != null) {
            for (Prefix prefix : prefixes) {
                prefix.closeTermEnum();
            }
        }
    }

    @Override
    public void reset(long term) {
        firstTerm = term;
        prefixQueue = null;
    }

    @Override
    public long term() {
        sanityCheck();
        return prefixQueue.element().val;
    }

    @Override
    public long getOffset() {
        sanityCheck();
        return prefixQueue.element().termEnum.getOffset();
    }

    private void sanityCheck() {
        if (prefixQueue == null || prefixQueue.isEmpty()) {
            throw new IllegalArgumentException("Invalid operation given iterators current state");
        }
    }
}

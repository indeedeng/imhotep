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
 package com.indeed.flamdex.lucene;

import com.indeed.flamdex.api.IntTermIterator;
import com.indeed.util.core.io.Closeables2;
import org.apache.log4j.Logger;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermEnum;

import javax.annotation.Nonnull;
import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.PriorityQueue;

class LuceneIntTermIterator implements IntTermIterator, LuceneTermIterator {
    private static final Logger LOGGER = Logger.getLogger(LuceneIntTermIterator.class);
    // only handles positive numbers
    private static final List<String> intPrefixes;

    static {
        final List<String> temp = new ArrayList<>();
        temp.add("0");
        // TODO: support negative values
        for (int i = 1; i <= 9; i++) {
            for (long m = 1; m > 0 && m*i > 0; m *= 10) {
                temp.add(String.valueOf(m * i));
            }
        }
        // should already be sorted but re-sorting in case I screwed up -ahudson
        Collections.sort(temp);
        intPrefixes = Collections.unmodifiableList(temp);
    }

    static class Prefix implements Comparable<Prefix>, Closeable {
        final IndexReader reader;
        final String field;
        final String firstTerm;
        final int length;
        final char prefix;

        TermEnum termEnum;
        boolean endOfStream = false;
        long val;

        public Prefix(final IndexReader reader, final String field, final String firstTerm) {
            this.reader = reader;
            this.field = field;
            this.prefix = firstTerm.charAt(0);
            this.length = firstTerm.length();
            this.firstTerm = firstTerm;
        }

        @Override
        public void close() throws IOException {
            closeTermEnum();
            endOfStream = false;
        }

        private void closeTermEnum() {
            if (termEnum != null) {
                try {
                    termEnum.close();
                } catch (final IOException e) {
                    throw LuceneUtils.ioRuntimeException(e);
                }
            }
            termEnum = null;
        }

        private void initialize() {
            try {
                termEnum = reader.terms(new Term(field, firstTerm));
            } catch (final IOException e) {
                throw LuceneUtils.ioRuntimeException(e);
            }
            if (termEnum.term() == null || !field.equals(termEnum.term().field()) || !firstTerm.equals(termEnum.term().text())) {
                throw new RuntimeException("Serious bug detected, term was "+termEnum.term()+", expected "+(new Term(field, firstTerm)));
            }
            val = Long.parseLong(firstTerm);
        }

        public boolean next() {
            if (endOfStream) {
                return false;
            }

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

                final boolean skipSuccess;
                try {
                    skipSuccess = termEnum.skipTo(new Term(field, nextTargetString));
                } catch (final IOException e) {
                    throw LuceneUtils.ioRuntimeException(e);
                }

                if (!skipSuccess ||
                        termEnum.term() == null ||
                        !field.equals(termEnum.term().field()) ||
                        termEnum.term().text().charAt(0) != prefix) {
                    closeTermEnum();
                    endOfStream = true;
                    return false;
                }

                final String currentTerm = termEnum.term().text();
                if (currentTerm.length() == length) {
                    val = Long.parseLong(termEnum.term().text());
                    // todo deal with potential parse int errors?
                    return true;
                }

                // length is either longer or shorter, either way find the next targetString w/ prefix and length
                if (currentTerm.length() > length) {
                    nextTargetString = Long.toString(Long.parseLong(currentTerm.substring(0, length))+1);
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
        public int compareTo(@Nonnull final Prefix o) {
            if (val < o.val) {
                return -1;
            }
            if (val > o.val) {
                return 1;
            }
            throw new RuntimeException("Impossible condition occurred");
        }
    }

    private final IndexReader reader;
    private final String field;
    private PriorityQueue<Prefix> prefixQueue;
    private List<Prefix> prefixes;
    private long firstTerm = 0;

    LuceneIntTermIterator(final IndexReader reader, final String field) {
        this.reader = reader;
        this.field = field;
    }

    private List<Prefix> determineAppropriatePrefixes() {
        final String[][] firstTerm = new String[19][10];
        try {
            final TermEnum termEnum = reader.terms(new Term(field, "0"));
            while (true) {
                final Term term = termEnum.term();
                if (term == null || !field.equals(term.field()) || term.text().charAt(0) > '9') {
                    break;
                }

                final String termText = term.text();
                final int x = termText.length()-1;
                if (x >= 0 && x < firstTerm.length) {
                    final int y = termText.charAt(0)-'0';
                    if (y >= 0 && y < firstTerm[x].length) {
                        if (firstTerm[x][y] == null) {
                            firstTerm[x][y] = termText;
                        }
                    }
                }

                if (!termEnum.next()) {
                    break;
                }
            }
            termEnum.close();
        } catch (final IOException e) {
            throw LuceneUtils.ioRuntimeException(e);
        }

        final List<Prefix> ret = new ArrayList<>();
        for (final String intPrefix : intPrefixes) {
            final int x = intPrefix.length()-1;
            final int y = intPrefix.charAt(0)-'0';
            if (firstTerm[x][y] != null) {
                ret.add(new Prefix(reader, field, firstTerm[x][y]));
                firstTerm[x][y] = null;
            }
        }

        return ret;
    }

    private void initialize(final long term) {
        if (prefixes == null) {
            prefixes = determineAppropriatePrefixes();
        } else {
            Closeables2.closeAll(prefixes, LOGGER);
        }
        if (prefixes.isEmpty()) {
            prefixQueue = new PriorityQueue<>(1);
            return;
        }
        prefixQueue = new PriorityQueue<>(prefixes.size());
        for (final Prefix prefix : prefixes) {
            if (prefix.next()) {
                prefixQueue.add(prefix);
            }
        }
        while (!prefixQueue.isEmpty()) {
            if (prefixQueue.element().val >= term) {
                break;
            }
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
            if (prefix.next()) {
                prefixQueue.add(prefix);
            }
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
        Closeables2.closeAll(prefixes, LOGGER);
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

    @Override
    public TermEnum termEnum() {
        sanityCheck();
        return prefixQueue.element().termEnum;
    }

    private void sanityCheck() {
        if (prefixQueue == null || prefixQueue.isEmpty()) {
            throw new IllegalArgumentException("Invalid operation given iterators current state");
        }
    }
}

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

package com.indeed.flamdex.simple;

import com.indeed.flamdex.api.IntTermIterator;
import com.indeed.flamdex.api.StringTermIterator;
import com.indeed.imhotep.scheduling.TaskScheduler;

import java.io.IOException;
import java.nio.file.Path;

/**
 * @author vladimir
 */

public class UnsortedStringToIntTermIterator<T extends StringTermIterator> implements IntTermIterator {
    protected final T stringTermIterator;
    private long currentTerm;

    public UnsortedStringToIntTermIterator(final T stringTermIterator) {
        this.stringTermIterator = stringTermIterator;
    }

    public T getStringTermIterator() {
        return stringTermIterator;
    }

    @Override
    public void reset(final long term) {
        stringTermIterator.reset(String.valueOf(term));
    }

    @Override
    public long term() {
        return currentTerm;
    }

    @Override
    public boolean next() {
        // searching for next string term that is convertible to long
        while (stringTermIterator.next()) {
            if (tryParseLong(stringTermIterator.termStringBytes(), stringTermIterator.termStringLength())) {
                return true;
            }

            // This method call could be long if we try to convert field with lot of string terms.
            // So yielding in case of failure.
            TaskScheduler.CPUScheduler.yieldIfNecessary();

            // So if we fail to parse, then move to next possible variant

            // allowed chars in string representation of a number are:
            // '+' - ASCII code 43
            // '-' - ASCII code 45
            // '0' - '9' - ASCII codes 48-57

            if (stringTermIterator.termStringLength() == 0) {
                // it was just empty string. let's see what's next.
                continue;
            }

            final byte firstChar = stringTermIterator.termStringBytes()[0];
            if (firstChar < '+') {
                // Skipping all terms until first lexicographically valid string.
                stringTermIterator.reset("+0");
                continue;
            }

            if (firstChar > '+' && firstChar < '-') { // one symbol actually ',' - code 44
                stringTermIterator.reset("-0");
                continue;
            }

            if (firstChar > '-' && firstChar < '0') {
                stringTermIterator.reset("0");
                continue;
            }

            if (firstChar > '9') {
                // There will be no numbers in this string iterator, finish iteration.
                return false;
            }

            // If we get here, then term has a prefix that is valid prefix for a number.
            // Theoretically, it's possible to find bad character in a term and do clever reset() to next valid
            // prefix, but it's probably not worth it.
        }
        return false;
    }

    @Override
    public int docFreq() {
        return stringTermIterator.docFreq();
    }

    @Override
    public void close() {
        stringTermIterator.close();
    }

    /**
     * This method is refactored {@link com.google.common.primitives.Longs#tryParse(String)}
     * Two things changed:
     * 1. Our version supports leading '+' sign
     * 2. It works on utf-8 bytes, not on chars
    */
    private boolean tryParseLong(final byte[] str, final int len) {
        if (len == 0) {
            return false;
        }
        int index = 0;
        boolean negative = false;
        if (str[0] == '-') {
            negative = true;
            index = 1;
        } else if (str[0] == '+') {
            index = 1;
        }
        if (index == len) {
            return false;
        }

        int digit = str[index++] - '0';
        if (digit < 0 || digit > 9) {
            return false;
        }
        long accum = -digit;
        while (index < len) {
            digit = str[index++] - '0';
            if (digit < 0 || digit > 9 || accum < Long.MIN_VALUE / 10) {
                return false;
            }
            accum *= 10;
            if (accum < Long.MIN_VALUE + digit) {
                return false;
            }
            accum -= digit;
        }

        if (negative) {
            currentTerm = accum;
            return true;
        } else if (accum == Long.MIN_VALUE) {
            return false;
        } else {
            currentTerm = -accum;
            return true;
        }
    }

    public static class SimpleUnsortedStringToIntTermIterator
            extends UnsortedStringToIntTermIterator<SimpleStringTermIterator>
            implements SimpleIntTermIterator {

        SimpleUnsortedStringToIntTermIterator(final SimpleStringTermIterator simpleIterator) {
            super(simpleIterator);
        }

        @Deprecated
        public long getDocListAddress() throws IOException {
            return stringTermIterator.getDocListAddress();
        }

        public Path getFilename() {
            return stringTermIterator.getFilename();
        }

        public long getOffset() {
            return stringTermIterator.getOffset();
        }
    }

}

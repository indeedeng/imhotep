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
            try {
                currentTerm = Long.parseLong(stringTermIterator.term());
                return true;
            } catch (final NumberFormatException ignored) {
            }
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

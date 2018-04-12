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

import java.io.IOException;
import java.nio.file.Path;

/**
 * @author vladimir
 */

public class UnsortedStringToIntTermIterator implements SimpleIntTermIterator  {
    private final SimpleStringTermIterator stringTermIterator;

    public UnsortedStringToIntTermIterator(final SimpleStringTermIterator stringTermIterator) {
        this.stringTermIterator = stringTermIterator;
    }

    @Override
    public void reset(final long term) {
        stringTermIterator.reset(String.valueOf(term));
    }

    @Override
    public long term() {
        try {
            return Long.parseLong(stringTermIterator.term());
        } catch(final NumberFormatException ignored) {
            return 0;
        }
    }

    @Override
    public Path getFilename() {
        return stringTermIterator.getFilename();
    }

    @Override
    public long getOffset() {
        return stringTermIterator.getOffset();
    }

    @Override
    public boolean next() {
        return stringTermIterator.next();
    }

    @Override
    public int docFreq() {
        return stringTermIterator.docFreq();
    }

    @Override
    public void close() {
        stringTermIterator.close();
    }

    @Override
    public long getDocListAddress() throws IOException {
        return stringTermIterator.getDocListAddress();
    }
}

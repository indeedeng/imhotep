/*
 * Copyright (C) 2014 Indeed Inc.
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
 package com.indeed.flamdex.api;

import com.indeed.util.core.io.Closeables2;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Arrays;

/**
 * @author jplaisance
 */
public final class GenericRawStringTermDocIterator implements RawStringTermDocIterator {

    private static final Logger log = Logger.getLogger(GenericRawStringTermDocIterator.class);

    private final RawStringTermIterator termIterator;

    private final DocIdStream docIdStream;

    public GenericRawStringTermDocIterator(RawStringTermIterator termIterator, DocIdStream docIdStream) {
        this.termIterator = termIterator;
        this.docIdStream = docIdStream;
    }

    @Override
    public boolean nextTerm() {
        final boolean ret = termIterator.next();
        if (ret) {
            docIdStream.reset(termIterator);
        }
        return ret;
    }

    @Override
    public String term() {
        return termIterator.term();
    }

    @Override
    public byte[] termStringBytes() {
        return termIterator.termStringBytes();
    }

    @Override
    public int termStringLength() {
        return termIterator.termStringLength();
    }

    @Override
    public int docFreq() {
        return termIterator.docFreq();
    }

    @Override
    public int fillDocIdBuffer(final int[] docIdBuffer) {
        return docIdStream.fillDocIdBuffer(docIdBuffer);
    }

    @Override
    public void close() throws IOException {
        Closeables2.closeAll(Arrays.asList(termIterator, docIdStream), log);
    }
}

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
 package com.indeed.flamdex.fieldcache;

import com.indeed.flamdex.api.DocIdStream;
import com.indeed.flamdex.api.FlamdexReader;
import com.indeed.flamdex.api.IntTermIterator;

/**
 * @author jsgroth
 */
public class UnsortedIntTermDocIteratorImpl implements UnsortedIntTermDocIterator {
    private final IntTermIterator iterator;
    private final DocIdStream docIdStream;

    public UnsortedIntTermDocIteratorImpl(IntTermIterator iterator, DocIdStream docIdStream) {
        this.iterator = iterator;
        this.docIdStream = docIdStream;
    }

    public static UnsortedIntTermDocIteratorImpl create(final FlamdexReader r, final String field) {
        final IntTermIterator iterator = r.getIntTermIterator(field);
        final DocIdStream docIdStream;
        try {
            docIdStream = r.getDocIdStream();
        } catch (RuntimeException e) {
            iterator.close();
            throw e;
        }
        return new UnsortedIntTermDocIteratorImpl(iterator, docIdStream);
    }

    @Override
    public boolean nextTerm() {
        if (iterator.next()) {
            docIdStream.reset(iterator);
            return true;
        }
        return false;
    }

    @Override
    public long term() {
        return iterator.term();
    }

    @Override
    public int nextDocs(int[] docIdBuffer) {
        return docIdStream.fillDocIdBuffer(docIdBuffer);
    }

    @Override
    public void close() {
        docIdStream.close();
        iterator.close();
    }
}

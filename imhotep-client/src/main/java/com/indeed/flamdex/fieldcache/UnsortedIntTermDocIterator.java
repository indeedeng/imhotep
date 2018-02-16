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

import java.io.Closeable;

/**
 * @author jsgroth
 *
 * A combination of {@link com.indeed.flamdex.api.IntTermIterator} and {@link com.indeed.flamdex.api.DocIdStream}
 * that does not guarantee that it will return terms in sorted order.
 *
 * Useful in the context of field caching because iterating over terms in a Lucene index in numerically sorted order
 * is very inefficient and not necessary for building a field cache.
 */
public interface UnsortedIntTermDocIterator extends Closeable {
    boolean nextTerm();
    long term();
    int docFreq();
    int nextDocs(int[] docIdBuffer);
    void close();
}

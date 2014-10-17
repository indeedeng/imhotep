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

import java.io.Closeable;

public interface DocIdStream extends Closeable {
    /**
     * Resets this DocIdStream to start streaming out docIds for the specified term
     * @param term The iterator pointing to the term of interest, iterator must be in a valid state 
     */
    void reset(TermIterator term);

    /**
     * @param docIdBuffer the buffer into which to put docIds
     * @return  How many docIds were put into the buffer, if this is less than the length of the buffer, the Stream is considered finished
     */
    int fillDocIdBuffer(int[] docIdBuffer);

    /**
     * closes any open resources
     */
    void close();
}

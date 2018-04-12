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
 package com.indeed.flamdex.api;

import java.io.Closeable;

public interface TermIterator extends Closeable {
    /**
     * @return true if iterator successfully moves to the next term, terms are always traversed in ascending order
     */
    boolean next();

    /**
     * @return how many documents are in the index that contain the current term, invalid if next() is not called or if next() returned false
     */
    int docFreq();

    /**
     * close any resources associated with this term iterator
     */
    void close();
}

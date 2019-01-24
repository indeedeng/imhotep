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

public interface StringTermIterator extends TermIterator {
    /**
     * Resets this iterator, so that the next time next() is called it will be positioned at the first term that is &gt;= provided term.  The iterator is
     * no longer valid until the next call to next()
     * @param term The term to reset the iterator to
     */
    void reset(String term);

    /**
     * @return  the current term, invalid before next() is called or if next() returned false
     */
    String term();

    /**
     * UTF-8 bytes for the current string term
     * It will generally return a reference to an internal array instead of a new copy, so if you're going to advance the
     * iterator before reading the bytes then you need to copy them into another array before calling next()
     * @return a reference to an internal byte array
     */
    byte[] termStringBytes();

    /**
     * @return the number of bytes in the return value of {@link #termStringBytes()}} used for the current term
     */
    int termStringLength();

    /**
     * Estimated number (in bytes) of the common prefix of this term and the previous term.
     * Estimated means the returned number could be lower than the actual.
     * It's safe to return 0 if you're implementation isn't optimized for this.
     * This method should return 0 for the first term, but can return positive number after reset().
     *
     * @return a non-negative number no larger than the common prefix length with the previous term.
     */
    int commonPrefixLengthWithPrevious();
}
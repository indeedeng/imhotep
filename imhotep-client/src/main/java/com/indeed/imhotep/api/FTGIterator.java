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
 package com.indeed.imhotep.api;

import com.indeed.imhotep.scheduling.SilentCloseable;

public interface FTGIterator extends SilentCloseable {
    /**
     * @return upper bound for number of groups.
     * It is guaranteed that values returned by {{@link #group()} method will be in range [1, getNumGroups() - 1],
     * but this method can return any value greater than or equal to `max(all results of {{@link #group()} method) + 1`
     */
    int getNumGroups();

    /**
     * @return true iff iterator successfully positioned to the next field
     */
    boolean nextField();

    /**
     * @return name of the current field
     */
    String fieldName();

    /**
     * @return true iff the current field is an int type
     */
    boolean fieldIsIntType();

    /**
     * @return true iff iterator successfully positioned to next term within current field
     */
    boolean nextTerm();

    /**
     * @return the number of documents to be iterated over for the current term
     */
    long termDocFreq();

    /**
     * @return the current term, if current field is an int type
     */
    long termIntVal();

    /**
     * @return the current term, if current field is a String type
     */
    String termStringVal();

    /**
     * This method returns the UTF-8 bytes for the current string term
     * It will generally return a reference to an internal array instead of a new copy, so if you're going to advance the
     * iterator before reading the bytes then you need to copy them into another array before calling any of the next* methods
     *
     * @return a reference to an internal byte array
     */
    byte[] termStringBytes();

    /**
     * @return the number of bytes in the return value of {@link #termStringBytes()}} used for the current term
     */
    int termStringLength();

    /**
     * @return true iff iterator succesfully positions to the next group within the current term
     */
    boolean nextGroup();

    /**
     * @return group id where iterator is positioned
     */
    int group();

    /**
     * close the iterator, this is only necessary if you want to stop the iterator before completely exhausting it
     * behavior is undefined if you call any other methods after calling close
     */
    void close();
}

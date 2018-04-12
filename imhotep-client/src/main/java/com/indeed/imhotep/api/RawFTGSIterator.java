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

/**
 * @author jsgroth
 *
 * an FTGS iterator that supports accessing the raw UTF-8 byte array for the current string term
 */
public interface RawFTGSIterator extends FTGSIterator {
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
}

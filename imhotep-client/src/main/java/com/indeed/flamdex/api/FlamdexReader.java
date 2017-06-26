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
import java.nio.file.Path;
import java.util.Collection;

public interface FlamdexReader extends Closeable {
    Collection<String> getIntFields();
    Collection<String> getStringFields();
    int getNumDocs();

    /** @return Path to directory with flamdex data.
     * Can be null in case of memory flamdex or another special flamdex */
    Path getDirectory();

    DocIdStream getDocIdStream();
    IntTermIterator getUnsortedIntTermIterator(String field);
    IntTermIterator getIntTermIterator(String field);
    StringTermIterator getStringTermIterator(String field);
    IntTermDocIterator getIntTermDocIterator(String field);
    StringTermDocIterator getStringTermDocIterator(String field);
    long getIntTotalDocFreq(String field);
    long getStringTotalDocFreq(String field);
    Collection<String> getAvailableMetrics();
    IntValueLookup getMetric(String metric) throws FlamdexOutOfMemoryException;
    StringValueLookup getStringLookup(String field) throws FlamdexOutOfMemoryException;
    long memoryRequired(String metric);
}

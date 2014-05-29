package com.indeed.flamdex.api;

import java.io.Closeable;
import java.util.Collection;

public interface FlamdexReader extends Closeable {
    Collection<String> getIntFields();
    Collection<String> getStringFields();
    int getNumDocs();
    String getDirectory();
    DocIdStream getDocIdStream();
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

package com.indeed.flamdex.writer;

import java.io.IOException;

/**
 * @author jsgroth
 */
public interface IntFieldWriter {
    void nextTerm(long term) throws IOException;
    void nextDoc(int doc) throws IOException;
    void close() throws IOException;
}

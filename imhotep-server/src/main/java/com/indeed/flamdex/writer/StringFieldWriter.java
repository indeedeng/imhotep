package com.indeed.flamdex.writer;

import java.io.IOException;

/**
 * @author jsgroth
 */
public interface StringFieldWriter {
    void nextTerm(String term) throws IOException;
    void nextDoc(int doc) throws IOException;
    void close() throws IOException;
}

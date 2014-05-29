package com.indeed.flamdex.writer;

import java.io.IOException;

/**
 * @author jsgroth
 */
public interface FlamdexWriter {
    String getOutputDirectory();
    void resetMaxDocs(long maxDocs);
    IntFieldWriter getIntFieldWriter(String field) throws IOException;
    StringFieldWriter getStringFieldWriter(String field) throws IOException;
    void close() throws IOException;
}

package com.indeed.flamdex.writer;

import java.io.IOException;

/**
 * @author jplaisance
 */
public interface IntFieldWriterFactory {
    public IntFieldWriter create(String outputDirectory, String field, long numDocs) throws IOException;
}

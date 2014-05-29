package com.indeed.flamdex.writer;

import java.io.IOException;

/**
 * @author jplaisance
 */
public interface StringFieldWriterFactory {
    public StringFieldWriter create(String outputDirectory, String field, long numDocs) throws IOException;
}

package com.indeed.flamdex.writer;

import java.io.Closeable;
import java.io.IOException;

/**
 * @author jsgroth
 */
public interface FlamdexDocWriter extends Closeable {
    void addDocument(FlamdexDocument doc) throws IOException;
}

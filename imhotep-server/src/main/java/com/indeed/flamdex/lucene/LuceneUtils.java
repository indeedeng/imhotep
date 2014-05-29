package com.indeed.flamdex.lucene;

import java.io.IOException;

class LuceneUtils {
    static RuntimeException ioRuntimeException(final IOException e) {
        return new RuntimeException("IOException in underlying lucene layer", e);
    }
}

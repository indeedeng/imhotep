package com.indeed.flamdex.dynamic;

import com.indeed.flamdex.query.Query;
import com.indeed.flamdex.writer.FlamdexDocWriter;

import javax.annotation.Nonnull;
import java.io.IOException;

/**
 * @author michihiko
 */
public interface DeletableFlamdexDocWriter extends FlamdexDocWriter {
    void deleteDocuments(@Nonnull final Query query) throws IOException;
}

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
 package com.indeed.flamdex.simple;

import com.indeed.flamdex.utils.FlamdexUtils;
import com.indeed.flamdex.writer.IntFieldWriter;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * @author jsgroth
 */
final class SimpleIntFieldWriter extends SimpleFieldWriter implements IntFieldWriter {
    private final Path outputDirectory;
    private final String field;
    private final boolean writeBTreesOnClose;

    private long lastTermWritten = 0L;
    private boolean hasCurrentTerm = false;
    private long currentTerm;

    private SimpleIntFieldWriter(Path outputDirectory,
                                 String field,
                                 boolean writeBTreesOnClose,
                                 OutputStream termsOutput,
                                 OutputStream docsOutput,
                                 long numDocs) {
        super(termsOutput, docsOutput, numDocs);
        this.outputDirectory = outputDirectory;
        this.field = field;
        this.writeBTreesOnClose = writeBTreesOnClose;
    }

    public static String getTermsFilename(String field) {
        return "fld-"+field+".intterms";
    }

    public static String get32IndexFilename(String field) {
        return "fld-"+field+".intindex";
    }

    public static String get64IndexFilename(String field) {
        return "fld-"+field+".intindex64";
    }

    public static String getDocsFilename(String field) {
        return "fld-"+field+".intdocs";
    }

    public static SimpleIntFieldWriter open(Path outputDirectory,
                                            String field,
                                            long numDocs,
                                            boolean writeBTreesOnClose) throws
            IOException {
        final OutputStream termsOutput;
        final OutputStream docsOutput;

        termsOutput = Files.newOutputStream(outputDirectory.resolve(getTermsFilename(field)));
        docsOutput = Files.newOutputStream(outputDirectory.resolve(getDocsFilename(field)));

        return new SimpleIntFieldWriter(outputDirectory,
                                        field,
                                        writeBTreesOnClose,
                                        new BufferedOutputStream(termsOutput, 65536),
                                        new BufferedOutputStream(docsOutput, 65536),
                                        numDocs);
    }

    /**
     * switch terms
     *
     * @param term the term to switch to
     * @throws IOException if there is a file write error
     * @throws IllegalArgumentException if term is negative or if term is less than or equal to the previous term added
     */
    @Override
    public void nextTerm(long term) throws IOException {
        if (hasCurrentTerm && term <= currentTerm) {
            throw new IllegalArgumentException(
                    "terms must be in sorted order: " + term + " is not greater than "
                            + currentTerm);
        }

        internalNextTerm();
        hasCurrentTerm = true;
        currentTerm = term;
    }

    @Override
    protected void writeTermDelta() throws IOException {
        final long termDelta = currentTerm - lastTermWritten;
        FlamdexUtils.writeVLong(termDelta, termsOutput);
        lastTermWritten = currentTerm;
    }

    @Override
    protected void writeBTreeIndex() throws IOException {
        if (writeBTreesOnClose) {
            final Path indexDir = outputDirectory.resolve("fld-" + field + ".intindex64");
            SimpleFlamdexWriter.writeIntBTree(outputDirectory,
                                              field,
                                              indexDir);
        }
    }
}

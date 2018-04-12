/*
 * Copyright (C) 2018 Indeed Inc.
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

import com.google.common.base.Charsets;
import com.indeed.flamdex.utils.FlamdexUtils;
import com.indeed.flamdex.writer.StringFieldWriter;
import com.indeed.util.core.io.Closeables2;
import org.apache.log4j.Logger;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * @author jsgroth
 */
class SimpleStringFieldWriter extends SimpleFieldWriter implements StringFieldWriter {
    private static final Logger LOGGER = Logger.getLogger(SimpleStringFieldWriter.class);
    private final Path outputDirectory;
    private final String field;
    private final boolean writeBTreesOnClose;

    private byte[] lastWrittenTermBytes = new byte[0];
    private String currentTerm = null;

    private SimpleStringFieldWriter(
            final Path outputDirectory,
            final String field,
            final boolean writeBTreesOnClose,
            final OutputStream termsOutput,
            final OutputStream docsOutput,
            final long numDocs) {
        super(termsOutput, docsOutput, numDocs);
        this.outputDirectory = outputDirectory;
        this.field = field;
        this.writeBTreesOnClose = writeBTreesOnClose;
    }

    public static String getTermsFilename(final String field) {
        return "fld-"+field+".strterms";
    }

    public static String getIndexFilename(final String field) {
        return "fld-"+field+".strindex";
    }

    public static String getDocsFilename(final String field) {
        return "fld-"+field+".strdocs";
    }

    public static SimpleStringFieldWriter open(final Path outputDirectory,
                                               final String field,
                                               final long numDocs,
                                               final boolean writeBTreesOnClose) throws IOException {
        final OutputStream termsOutput;
        final OutputStream docsOutput;

        termsOutput = Files.newOutputStream(outputDirectory.resolve(getTermsFilename(field)));
        try {
            docsOutput = Files.newOutputStream(outputDirectory.resolve(getDocsFilename(field)));
        } catch (final IOException e) {
            Closeables2.closeQuietly(termsOutput, LOGGER);
            throw e;
        }

        return new SimpleStringFieldWriter(outputDirectory,
                                           field,
                                           writeBTreesOnClose,
                                           new BufferedOutputStream(termsOutput, 65536),
                                           new BufferedOutputStream(docsOutput, 65536),
                                           numDocs);
    }

    /**
     * switch terms
     *
     * @param term the next term to write to the index
     * @throws IOException if there is a file write error
     * @throws NullPointerException if term is null
     * @throws IllegalArgumentException if term is not lexicographically greater than the previous term added
     */
    @Override
    public void nextTerm(final String term) throws IOException {
        if (term == null) {
            throw new NullPointerException("you just had to try, didn't you?");
        }
        if (currentTerm != null && currentTerm.compareTo(term) >= 0) {
            throw new IllegalArgumentException("terms must be in sorted order: " + term
                                                       + " is not lexicographically greater than "
                                                       + currentTerm);
        }
        
        internalNextTerm();
        currentTerm = term;
    }

    @Override
    protected void writeTermDelta() throws IOException {
        final byte[] currentTermBytes = currentTerm.getBytes(Charsets.UTF_8);
        final int prefixLen = getPrefixLen(lastWrittenTermBytes,
                                           currentTermBytes,
                                           Math.min(lastWrittenTermBytes.length,
                                                    currentTermBytes.length));
        FlamdexUtils.writeVLong(lastWrittenTermBytes.length - prefixLen, termsOutput);
        FlamdexUtils.writeVLong(currentTermBytes.length - prefixLen, termsOutput);
        termsOutput.write(currentTermBytes, prefixLen, currentTermBytes.length - prefixLen);
        lastWrittenTermBytes = currentTermBytes;
    }

    @Override
    protected void writeBTreeIndex() throws IOException {
        if (writeBTreesOnClose) {
            final Path btreeDir = outputDirectory.resolve(getIndexFilename(field));
            SimpleFlamdexWriter.writeStringBTree(outputDirectory,
                                                 field,
                                                 btreeDir);
        }
    }

    private static int getPrefixLen(final byte[] a, final byte[] b, final int n) {
        for (int i = 0; i < n; ++i) {
            if (a[i] != b[i]) {
                return i;
            }
        }
        return n;
    }
}

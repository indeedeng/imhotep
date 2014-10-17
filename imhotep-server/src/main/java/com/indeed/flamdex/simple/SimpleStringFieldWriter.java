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

import com.google.common.base.Charsets;
import com.indeed.util.io.Files;
import com.indeed.flamdex.utils.FlamdexUtils;
import com.indeed.flamdex.writer.StringFieldWriter;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;

/**
 * @author jsgroth
 */
class SimpleStringFieldWriter extends SimpleFieldWriter implements StringFieldWriter {
    private final String outputDirectory;
    private final String field;
    private final boolean writeBTreesOnClose;

    private byte[] lastWrittenTermBytes = new byte[0];
    private String currentTerm = null;

    private SimpleStringFieldWriter(String outputDirectory, String field, boolean writeBTreesOnClose, OutputStream termsOutput, OutputStream docsOutput, long numDocs) {
        super(termsOutput, docsOutput, numDocs);
        this.outputDirectory = outputDirectory;
        this.field = field;
        this.writeBTreesOnClose = writeBTreesOnClose;
    }

    public static String getTermsFilename(String field) {
        return "fld-"+field+".strterms";
    }

    public static String getDocsFilename(String field) {
        return "fld-"+field+".strdocs";
    }

    public static SimpleStringFieldWriter open(String outputDirectory, String field, long numDocs, boolean writeBTreesOnClose) throws FileNotFoundException {
        final OutputStream termsOutput = new BufferedOutputStream(new FileOutputStream(Files.buildPath(outputDirectory, getTermsFilename(field))), 65536);
        final OutputStream docsOutput = new BufferedOutputStream(new FileOutputStream(Files.buildPath(outputDirectory, getDocsFilename(field))), 65536);
        return new SimpleStringFieldWriter(outputDirectory, field, writeBTreesOnClose, termsOutput, docsOutput, numDocs);
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
    public void nextTerm(String term) throws IOException {
        if (term == null) throw new NullPointerException("you just had to try, didn't you?");
        if (currentTerm != null && currentTerm.compareTo(term) >= 0) {
            throw new IllegalArgumentException("terms must be in sorted order: "+term+" is not lexicographically greater than "+currentTerm);
        }
        
        internalNextTerm();
        currentTerm = term;
    }

    @Override
    protected void writeTermDelta() throws IOException {
        final byte[] currentTermBytes = currentTerm.getBytes(Charsets.UTF_8);
        final int prefixLen = getPrefixLen(lastWrittenTermBytes, currentTermBytes, Math.min(lastWrittenTermBytes.length, currentTermBytes.length));
        FlamdexUtils.writeVLong(lastWrittenTermBytes.length - prefixLen, termsOutput);
        FlamdexUtils.writeVLong(currentTermBytes.length - prefixLen, termsOutput);
        termsOutput.write(currentTermBytes, prefixLen, currentTermBytes.length - prefixLen);
        lastWrittenTermBytes = currentTermBytes;
    }

    @Override
    protected void writeBTreeIndex() throws IOException {
        if (writeBTreesOnClose) {
            SimpleFlamdexWriter.writeStringBTree(outputDirectory, field, new File(outputDirectory, "fld-" + field + ".strindex"));
        }
    }

    private static int getPrefixLen(byte[] a, byte[] b, int n) {
        for (int i = 0; i < n; ++i) {
            if (a[i] != b[i]) return i;
        }
        return n;
    }
}

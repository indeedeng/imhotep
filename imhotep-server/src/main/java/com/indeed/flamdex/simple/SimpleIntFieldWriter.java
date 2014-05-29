package com.indeed.flamdex.simple;

import com.indeed.util.io.Files;
import com.indeed.flamdex.utils.FlamdexUtils;
import com.indeed.flamdex.writer.IntFieldWriter;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;

/**
 * @author jsgroth
 */
final class SimpleIntFieldWriter extends SimpleFieldWriter implements IntFieldWriter {
    private final String outputDirectory;
    private final String field;
    private final boolean writeBTreesOnClose;

    private long lastTermWritten = 0L;
    private boolean hasCurrentTerm = false;
    private long currentTerm;

    private SimpleIntFieldWriter(String outputDirectory, String field, boolean writeBTreesOnClose, OutputStream termsOutput, OutputStream docsOutput, long numDocs) {
        super(termsOutput, docsOutput, numDocs);
        this.outputDirectory = outputDirectory;
        this.field = field;
        this.writeBTreesOnClose = writeBTreesOnClose;
    }

    public static String getTermsFilename(String field) {
        return "fld-"+field+".intterms";
    }

    public static String getDocsFilename(String field) {
        return "fld-"+field+".intdocs";
    }

    public static SimpleIntFieldWriter open(String outputDirectory, String field, long numDocs, boolean writeBTreesOnClose) throws FileNotFoundException {
        final OutputStream termsOutput = new BufferedOutputStream(new FileOutputStream(Files.buildPath(outputDirectory, getTermsFilename(field))), 65536);
        final OutputStream docsOutput = new BufferedOutputStream(new FileOutputStream(Files.buildPath(outputDirectory, getDocsFilename(field))), 65536);
        return new SimpleIntFieldWriter(outputDirectory, field, writeBTreesOnClose, termsOutput, docsOutput, numDocs);
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
        if (hasCurrentTerm && term <= currentTerm) throw new IllegalArgumentException("terms must be in sorted order: "+term+" is not greater than "+currentTerm);

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
            SimpleFlamdexWriter.writeIntBTree(outputDirectory, field, new File(outputDirectory, "fld-" + field + ".intindex64"));
        }
    }
}

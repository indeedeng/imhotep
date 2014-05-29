package com.indeed.flamdex.ramses;

import com.google.common.base.Throwables;
import com.indeed.util.io.Files;
import com.indeed.flamdex.api.DocIdStream;
import com.indeed.flamdex.api.FlamdexOutOfMemoryException;
import com.indeed.flamdex.api.FlamdexReader;
import com.indeed.flamdex.api.IntTermDocIterator;
import com.indeed.flamdex.api.IntTermIterator;
import com.indeed.flamdex.api.IntValueLookup;
import com.indeed.flamdex.api.StringTermDocIterator;
import com.indeed.flamdex.api.StringTermIterator;
import com.indeed.flamdex.api.StringValueLookup;
import com.indeed.flamdex.fieldcache.FieldCacher;
import com.indeed.imhotep.metrics.Count;

import java.io.File;
import java.io.IOException;
import java.util.Collection;

/**
 * @author jsgroth
 */
public class RamsesFlamdexWrapper implements FlamdexReader {
    private static final String TIME_UPPER_BITS_FILE = "timeupperbits.bin";
    private static final String DOC_ID_BOUNDARIES_FILE = "tubdocids.bin";
    private static final String TIME_LOWER_BITS_FILE = "timelowerbits.bin";
    private static final String SCALE_FACTOR_FILE = "scale.bin";

    private final FlamdexReader wrapped;
    private final String directory;

    private final int scaleFactor;

    private final long memoryOverhead;

    public RamsesFlamdexWrapper(FlamdexReader wrapped, String directory) {
        this.wrapped = wrapped;
        this.directory = directory;

        memoryOverhead = new File(directory, TIME_UPPER_BITS_FILE).length() +
                new File(directory, DOC_ID_BOUNDARIES_FILE).length() +
                new File(directory, TIME_LOWER_BITS_FILE).length();

        final Integer rawScaleFactor = Files.readObjectFromFile(Files.buildPath(directory, SCALE_FACTOR_FILE), Integer.class);
        scaleFactor = rawScaleFactor != null ? rawScaleFactor : 1;
    }

    @Override
    public Collection<String> getIntFields() {
        return wrapped.getIntFields();
    }

    @Override
    public Collection<String> getStringFields() {
        return wrapped.getStringFields();
    }

    @Override
    public int getNumDocs() {
        return wrapped.getNumDocs();
    }

    @Override
    public String getDirectory() {
        return wrapped.getDirectory();
    }

    @Override
    public DocIdStream getDocIdStream() {
        return wrapped.getDocIdStream();
    }

    @Override
    public IntTermIterator getIntTermIterator(String field) {
        return wrapped.getIntTermIterator(field);
    }

    @Override
    public StringTermIterator getStringTermIterator(String field) {
        return wrapped.getStringTermIterator(field);
    }

    @Override
    public IntTermDocIterator getIntTermDocIterator(final String field) {
        return wrapped.getIntTermDocIterator(field);
    }

    @Override
    public StringTermDocIterator getStringTermDocIterator(final String field) {
        return wrapped.getStringTermDocIterator(field);
    }

    @Override
    public long getIntTotalDocFreq(String field) {
        return wrapped.getIntTotalDocFreq(field);
    }

    @Override
    public long getStringTotalDocFreq(String field) {
        return wrapped.getStringTotalDocFreq(field);
    }

    @Override
    public Collection<String> getAvailableMetrics() {
        return wrapped.getAvailableMetrics();
    }

    @Override
    public IntValueLookup getMetric(String metric) throws FlamdexOutOfMemoryException {
        if ("time".equals(metric)) {
            return newTimeLookup();
        }
        final IntValueLookup rawMetric = "counts".equals(metric) ? new Count() : wrapped.getMetric(metric);
        return scaleFactor != 1 ? new ScalingMetric(rawMetric, scaleFactor) : rawMetric;
    }

    public StringValueLookup getStringLookup(final String field) throws FlamdexOutOfMemoryException {
        try {
            return FieldCacher.newStringValueLookup(field, this, directory);
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    private IntValueLookup newTimeLookup() {
        final int[] timeUpperBits = Files.readObjectFromFile(Files.buildPath(directory, TIME_UPPER_BITS_FILE), int[].class);
        final int[] docIdBoundaries = Files.readObjectFromFile(Files.buildPath(directory, DOC_ID_BOUNDARIES_FILE), int[].class);
        final byte[] timeLowerBits = Files.readObjectFromFile(Files.buildPath(directory, TIME_LOWER_BITS_FILE), byte[].class);

        if (timeUpperBits == null || docIdBoundaries == null || timeLowerBits == null) {
            throw new RuntimeException("unable to load ramses time metric from directory " + directory + ", missing one or more required files");
        }

        return new RamsesTimeIntValueLookup(timeUpperBits, docIdBoundaries, timeLowerBits, memoryOverhead);
    }

    @Override
    public long memoryRequired(String metric) {
        if ("time".equals(metric)) {
            return memoryOverhead;
        } else if ("counts".equals(metric)) {
            return 0;
        }
        return wrapped.memoryRequired(metric);
    }

    @Override
    public void close() throws IOException {
        wrapped.close();
    }

    public static boolean ramsesFilesExist(String dir) {
        final File f = new File(dir);
        return new File(f, TIME_UPPER_BITS_FILE).exists() &&
                new File(f, DOC_ID_BOUNDARIES_FILE).exists() &&
                new File(f, TIME_LOWER_BITS_FILE).exists();
    }
}

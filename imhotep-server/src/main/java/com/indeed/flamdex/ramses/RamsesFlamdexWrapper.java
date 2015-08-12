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
 package com.indeed.flamdex.ramses;

import com.google.common.base.Throwables;
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
import com.indeed.imhotep.io.caching.CachedFile;
import com.indeed.imhotep.metrics.Count;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
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

    public RamsesFlamdexWrapper(FlamdexReader wrapped, String directory) throws IOException {
        this.wrapped = wrapped;
        this.directory = directory;

        final CachedFile tubFile = CachedFile.create(CachedFile.buildPath(directory, TIME_UPPER_BITS_FILE));
        final CachedFile docIdFile = CachedFile.create(CachedFile.buildPath(directory, DOC_ID_BOUNDARIES_FILE));
        final CachedFile tlbFile = CachedFile.create(CachedFile.buildPath(directory, TIME_LOWER_BITS_FILE));
        memoryOverhead = tubFile.length() + docIdFile.length() + tlbFile.length();

        final File sfFile = CachedFile.create(CachedFile.buildPath(directory, SCALE_FACTOR_FILE)).loadFile();
        final Integer rawScaleFactor = readObjectFromFile(sfFile, Integer.class);
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
    public IntTermIterator getUnsortedIntTermIterator(String field) {
        return wrapped.getUnsortedIntTermIterator(field);
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
        
        try {
            final File tubFile = CachedFile.create(CachedFile.buildPath(directory, TIME_UPPER_BITS_FILE)).loadFile();
            final int[] timeUpperBits = readObjectFromFile(tubFile, int[].class);
            final File docIdFile = CachedFile.create(CachedFile.buildPath(directory, DOC_ID_BOUNDARIES_FILE)).loadFile();
            final int[] docIdBoundaries = readObjectFromFile(docIdFile, int[].class);
            final File tlbFile = CachedFile.create(CachedFile.buildPath(directory, TIME_LOWER_BITS_FILE)).loadFile();
            final byte[] timeLowerBits = readObjectFromFile(tlbFile, byte[].class);
    
            if (timeUpperBits == null || docIdBoundaries == null || timeLowerBits == null) {
                throw new RuntimeException("unable to load ramses time metric from directory " + directory + ", missing one or more required files");
            }
        
            return new RamsesTimeIntValueLookup(timeUpperBits, docIdBoundaries, timeLowerBits, memoryOverhead);

        } catch(IOException e) {
            throw new RuntimeException("unable to load ramses time metric from directory " + directory + ", missing one or more required files");
        }
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
        return CachedFile.create(CachedFile.buildPath(dir, TIME_UPPER_BITS_FILE)).exists() &&
                CachedFile.create(CachedFile.buildPath(dir, DOC_ID_BOUNDARIES_FILE)).exists() &&
                CachedFile.create(CachedFile.buildPath(dir, TIME_LOWER_BITS_FILE)).exists();
    }
    
    /**
     * Reads an object of type {@code T} from {@code file}.
     *
     * @param file file from which the object should be read
     * @param clazz non-null Class object for {@code T}
     * @param printException whether or not any stacktraces should be printed
     * @param <T> the return type
     * @return possibly null object of type {@code T}.
     */
    private static <T> T readObjectFromFile(File file, Class<T> clazz) {
        final FileInputStream fileIn;
        try {
            fileIn = new FileInputStream(file);
        } catch (Exception e) {
            return null;
        }

        final BufferedInputStream bufferedIn = new BufferedInputStream(fileIn);
        final ObjectInputStream objIn;
        try {
            objIn = new ObjectInputStream(bufferedIn);
        } catch (Exception e) {
            try {
                fileIn.close();
            } catch (IOException e1) { }
            return null;
        }

        final Object ret;
        try {
            ret = objIn.readObject();
        } catch (Exception e) {
            try {
                objIn.close();          // objIn.close() also closes fileIn
            } catch (IOException e2) { }
            return null;
        }

        try {
            objIn.close();          // objIn.close() also closes fileIn
        } catch (IOException e) { }
        return clazz.cast(ret);
    }
    
}

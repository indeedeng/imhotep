package com.indeed.flamdex.dynamic;

import com.indeed.flamdex.api.DocIdStream;
import com.indeed.flamdex.api.FlamdexOutOfMemoryException;
import com.indeed.flamdex.api.FlamdexReader;
import com.indeed.flamdex.api.IntTermDocIterator;
import com.indeed.flamdex.api.IntTermIterator;
import com.indeed.flamdex.api.IntValueLookup;
import com.indeed.flamdex.api.StringTermDocIterator;
import com.indeed.flamdex.api.StringTermIterator;
import com.indeed.flamdex.api.StringValueLookup;
import com.indeed.flamdex.api.TermIterator;
import com.indeed.flamdex.datastruct.FastBitSet;
import com.indeed.flamdex.dynamic.locks.MultiThreadFileLockUtil;
import com.indeed.flamdex.dynamic.locks.MultiThreadLock;
import com.indeed.flamdex.simple.SimpleFlamdexReader;

import javax.annotation.Nonnull;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collection;

/**
 * Simple implementation of {@link FlamdexReader} with tombstone (bitset for deleted docIds).
 *
 * @author michihiko
 */

class SegmentReader implements FlamdexReader {
    private static final String TOMBSTONE_FILENAME = "tombstone";
    private static final String SEGMENT_LOCK_FILENAME = "reader.lock";

    private final FlamdexReader flamdexReader;
    private final FastBitSet deleted;
    private final MultiThreadLock readerLock;

    SegmentReader(@Nonnull final Path directory, @Nonnull final String segment) throws IOException {
        final Path segmentDirectory = directory.resolve(segment);
        readerLock = MultiThreadFileLockUtil.readLock(segmentDirectory, SEGMENT_LOCK_FILENAME);
        flamdexReader = SimpleFlamdexReader.open(segmentDirectory);

        FastBitSet deleted = null;
        final File tombstoneFile = segmentDirectory.resolve(TOMBSTONE_FILENAME).toFile();
        if (tombstoneFile.exists() && tombstoneFile.isFile()) {
            try (final ObjectInputStream ois = new ObjectInputStream(new FileInputStream(tombstoneFile))) {
                deleted = (FastBitSet) ois.readObject();
            } catch (final ClassNotFoundException e) {
                throw new IOException(e);
            }
        }
        this.deleted = deleted;
    }

    @Override
    public void close() throws IOException {
        flamdexReader.close();
        readerLock.close();
    }

    @Override
    public Collection<String> getIntFields() {
        return flamdexReader.getIntFields();
    }

    @Override
    public Collection<String> getStringFields() {
        return flamdexReader.getStringFields();
    }

    int maxNumDocs() {
        return flamdexReader.getNumDocs();
    }

    @Override
    public int getNumDocs() {
        throw new UnsupportedOperationException("We don't need to implement this method");
    }

    @Override
    public Path getDirectory() {
        return flamdexReader.getDirectory();
    }

    @Override
    public DocIdStream getDocIdStream() {
        return new DocIdStream() {
            final DocIdStream docIdStream = flamdexReader.getDocIdStream();
            int bufferPos = 0;
            int bufferLength = 0;
            int[] internalBuffer = null;

            @Override
            public void reset(final TermIterator term) {
                docIdStream.reset(term);
            }

            @Override
            public int fillDocIdBuffer(final int[] docIdBuffer) {
                if (internalBuffer == null) {
                    internalBuffer = new int[docIdBuffer.length];
                } else if (internalBuffer.length < (docIdBuffer.length / 2)) {
                    internalBuffer = Arrays.copyOf(internalBuffer, docIdBuffer.length);
                }
                int numFilled = 0;
                while (numFilled < docIdBuffer.length) {
                    if (bufferPos == bufferLength) {
                        bufferPos = 0;
                        bufferLength = docIdStream.fillDocIdBuffer(internalBuffer);
                        if (bufferLength == 0) {
                            break;
                        }
                    }
                    final int docId = internalBuffer[bufferPos];
                    bufferPos++;
                    if ((deleted == null) || !deleted.get(docId)) {
                        docIdBuffer[numFilled] = docId;
                        numFilled++;
                    }
                }
                return numFilled;
            }

            @Override
            public void close() {
                docIdStream.close();
            }
        };
    }

    @Override
    public IntTermIterator getUnsortedIntTermIterator(final String field) {
        throw new UnsupportedOperationException("We don't need to implement this method");
    }

    @Override
    public IntTermIterator getIntTermIterator(final String field) {
        return flamdexReader.getIntTermIterator(field);
    }

    @Override
    public StringTermIterator getStringTermIterator(final String field) {
        return flamdexReader.getStringTermIterator(field);
    }

    @Override
    public IntTermDocIterator getIntTermDocIterator(final String field) {
        throw new UnsupportedOperationException("We don't need to implement this method");
    }

    @Override
    public StringTermDocIterator getStringTermDocIterator(final String field) {
        throw new UnsupportedOperationException("We don't need to implement this method");
    }

    @Override
    public long memoryRequired(final String metric) {
        return flamdexReader.memoryRequired(metric);
    }

    @Override
    public long getIntTotalDocFreq(final String field) {
        return flamdexReader.getIntTotalDocFreq(field);
    }

    @Override
    public long getStringTotalDocFreq(final String field) {
        return flamdexReader.getStringTotalDocFreq(field);
    }

    @Override
    public Collection<String> getAvailableMetrics() {
        return flamdexReader.getAvailableMetrics();
    }

    @Override
    public IntValueLookup getMetric(final String metric) throws FlamdexOutOfMemoryException {
        return flamdexReader.getMetric(metric);
    }

    @Override
    public StringValueLookup getStringLookup(final String field) throws FlamdexOutOfMemoryException {
        return flamdexReader.getStringLookup(field);
    }
}

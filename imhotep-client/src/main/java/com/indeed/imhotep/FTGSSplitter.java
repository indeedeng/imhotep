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
 package com.indeed.imhotep;

import com.indeed.imhotep.FTGSBinaryFormat.FieldStat;
import com.indeed.imhotep.api.FTGSIterator;
import com.indeed.imhotep.io.MultiFile;
import com.indeed.imhotep.io.TempFileSizeLimitExceededException;
import com.indeed.imhotep.io.WriteLimitExceededException;
import com.indeed.imhotep.scheduling.SilentCloseable;
import com.indeed.imhotep.service.FTGSOutputStreamWriter;
import com.indeed.imhotep.utils.tempfiles.ImhotepTempFiles;
import com.indeed.imhotep.utils.tempfiles.TempFile;
import com.indeed.util.core.Throwables2;
import com.indeed.util.core.hash.MurmurHash;
import com.indeed.util.core.io.Closeables2;
import org.apache.log4j.Logger;

import javax.annotation.WillClose;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
* @author jplaisance
*/
public final class FTGSSplitter implements Closeable {
    private static final Logger log = Logger.getLogger(FTGSSplitter.class);

    private final FTGSIterator iterator;

    private final int numSplits;

    private final OutputStream[] outputStreams;
    private final FTGSIterator[] ftgsIterators;
    private final SilentCloseable fileHandle;

    private final AtomicBoolean done = new AtomicBoolean(false);

    private final int largePrime;

    public FTGSSplitter(@WillClose final FTGSIterator ftgsIterator,
                        final int numSplits,
                        final int largePrime,
                        final AtomicLong tempFileSizeBytesLeft) throws IOException {
        this.iterator = ftgsIterator;
        this.numSplits = numSplits;
        this.largePrime = largePrime;
        outputStreams = new OutputStream[numSplits];
        try {
            final TempFile tempFile = ImhotepTempFiles.createFTGSSplitterTempFile();
            final MultiFile multiFile;
            try {
                multiFile = MultiFile.create(tempFile.unsafeGetFile(), numSplits, 256 * 1024, tempFileSizeBytesLeft);
                fileHandle = tempFile.addReference();
            } finally {
                tempFile.removeFileStillReferenced();
            }
            for (int i = 0; i < numSplits; i++) {
                outputStreams[i] = multiFile.getOutputStream(i);
            }
            ftgsIterators = run(multiFile);
        } catch (final Throwable t) {
            try {
                close();
            } finally {
                throw Throwables2.propagate(t, IOException.class);
            }
        }
    }

    public static FTGSIterator[] doSplit(@WillClose final FTGSIterator ftgsIterator,
                                         final int numSplits,
                                         final int largePrime,
                                         final AtomicLong tempFileSizeBytesLeft) throws IOException {
        final FTGSSplitter splitter = new FTGSSplitter(ftgsIterator, numSplits, largePrime, tempFileSizeBytesLeft);
        return splitter.getFtgsIterators();
    }

    public FTGSIterator[] getFtgsIterators() {
        return ftgsIterators;
    }

    private FTGSIterator[] run(final MultiFile multiFile) throws IOException {
        final FTGSOutputStreamWriter[] outputs = new FTGSOutputStreamWriter[numSplits];
        for (int i = 0; i < numSplits; i++) {
            outputs[i] = new FTGSOutputStreamWriter(outputStreams[i]);
        }

        final FTGSIterator[] iterators = new FTGSIterator[numSplits];
        try {
            final long[] statBuf = new long[iterator.getNumStats()];
            while (iterator.nextField()) {
                final boolean fieldIsIntType = iterator.fieldIsIntType();
                for (final FTGSOutputStreamWriter output : outputs) {
                    output.switchField(iterator.fieldName(), fieldIsIntType);
                }

                while (iterator.nextTerm()) {
                    final FTGSOutputStreamWriter output;
                    final int split;
                    if (fieldIsIntType) {
                        final long term = iterator.termIntVal();
                        split = (int) ((term*largePrime+12345 & Integer.MAX_VALUE) >> 16) % numSplits;
                        output = outputs[split];
                        output.switchIntTerm(term, iterator.termDocFreq());
                    } else {
                        split = hashStringTerm(iterator.termStringBytes(),
                                               iterator.termStringLength());
                        output = outputs[split];
                        output.switchBytesTerm(iterator.termStringBytes(),
                                               iterator.termStringLength(),
                                               iterator.termDocFreq());
                    }
                    while (iterator.nextGroup()) {
                        output.switchGroup(iterator.group());
                        iterator.groupStats(statBuf);
                        for (final long stat : statBuf) {
                            output.addStat(stat);
                        }
                    }
                }
            }
            final AtomicInteger doneCounter = new AtomicInteger();
            for (int i = 0; i < iterators.length; i++) {
                final FieldStat[] fieldStats = outputs[i].closeAndGetStats();
                iterators[i] = new SplitterFTGSIterator(multiFile.getInputStream(i), fieldStats, iterator.getNumStats(), iterator.getNumGroups(), doneCounter);
            }
            return iterators;
        } catch (final Throwable t) {
            Closeables2.closeAll(log, iterators);
            multiFile.cleanupQuietly();
            try {
                close();
            } finally {
                if (t instanceof WriteLimitExceededException) {
                    throw new TempFileSizeLimitExceededException(t);
                }
                throw Throwables2.propagate(t, IOException.class);
            }
        } finally {
            Closeables2.closeQuietly(iterator, log);
        }
    }

    private int hashStringTerm(final byte[] termStringBytes, final int termStringLength) {
        return ((MurmurHash.hash32(termStringBytes, 0, termStringLength) *
                 largePrime+12345 & 0x7FFFFFFF) >> 16) % numSplits;
    }

    public void close() {
        if (done.compareAndSet(false, true)) {
            final Closeable closeIterators = Closeables2.forArray(log, ftgsIterators);
            final Closeable closeOutputStreams = Closeables2.forArray(log, outputStreams);
            Closeables2.closeAll(log, iterator, closeIterators, closeOutputStreams, fileHandle);
        }
    }

    public boolean isClosed() {
        return done.get();
    }

    private class SplitterFTGSIterator extends InputStreamFTGSIterator {
        private boolean closed = false;
        private AtomicInteger doneCounter;

        SplitterFTGSIterator(final InputStream in,
                             final FieldStat[] fieldsStats,
                             final int numStats,
                             final int numGroups,
                             final AtomicInteger doneCounter) {
            super(in, fieldsStats, numStats, numGroups);
            this.doneCounter = doneCounter;
        }

        @Override
        public void close() {
            if (!closed) {
                closed = true;
                super.close();
                if (doneCounter.incrementAndGet() == FTGSSplitter.this.numSplits) {
                    FTGSSplitter.this.close();
                }
            }
        }
    }
}

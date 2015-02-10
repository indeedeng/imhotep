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
 package com.indeed.imhotep;

import com.google.common.base.Charsets;
import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterables;
import com.indeed.util.core.Throwables2;
import com.indeed.util.core.hash.MurmurHash;
import com.indeed.imhotep.api.FTGSIterator;
import com.indeed.imhotep.api.RawFTGSIterator;
import com.indeed.imhotep.service.FTGSOutputStreamWriter;
import com.indeed.util.core.io.Closeables2;
import org.apache.log4j.Logger;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
* @author jplaisance
*/
public final class FTGSSplitter implements Runnable, Closeable {
    private static final Logger log = Logger.getLogger(FTGSSplitter.class);

    private final FTGSIterator iterator;

    private final int numSplits;

    private final FTGSOutputStreamWriter[] outputs;
    private final File[] files;
    private final OutputStream[] outputStreams;
    private final RawFTGSIterator[] ftgsIterators;

    private final AtomicBoolean done = new AtomicBoolean(false);

    private final Thread runThread;

    private final int numStats;
    private final int largePrime;

    public FTGSSplitter(FTGSIterator ftgsIterator, final int numSplits, final int numStats, final String threadNameSuffix, final int largePrime) throws IOException {
        this.iterator = ftgsIterator;
        this.numSplits = numSplits;
        this.numStats = numStats;
        this.largePrime = largePrime;
        outputs = new FTGSOutputStreamWriter[numSplits];
        files = new File[numSplits];
        outputStreams = new OutputStream[numSplits];
        ftgsIterators = new RawFTGSIterator[numSplits];
        final AtomicInteger doneCounter = new AtomicInteger();
        try {
            for (int i = 0; i < numSplits; i++) {
                files[i] = File.createTempFile("ftgsSplitter", ".tmp");
                outputStreams[i] = new BufferedOutputStream(new FileOutputStream(files[i]), 65536);
                outputs[i] = new FTGSOutputStreamWriter(outputStreams[i]);
                final int splitIndex = i;
                ftgsIterators[i] = new RawFTGSIterator() {

                    InputStreamFTGSIterator delegate = null;


                    @Override
                    public boolean nextField() {
                        try {
                            if (delegate == null) {
                                try {
                                    runThread.join();
                                } catch (InterruptedException e) {
                                    throw Throwables.propagate(e);
                                }

                                delegate = new InputStreamFTGSIterator(new BufferedInputStream(new FileInputStream(files[splitIndex]), 65536), numStats) {
                                    boolean closed = false;

                                    @Override
                                    public void close() {
                                        if (!closed) {
                                            closed = true;
                                            super.close();
                                            if (doneCounter.incrementAndGet() == numSplits) {
                                                FTGSSplitter.this.close();
                                            }
                                        }
                                    }
                                };
                                files[splitIndex].delete();
                            }
                            return delegate.nextField();
                        } catch (IOException e) {
                            throw Throwables.propagate(e);
                        }
                    }

                    @Override
                    public String fieldName() {
                        return delegate.fieldName();
                    }

                    @Override
                    public boolean fieldIsIntType() {
                        return delegate.fieldIsIntType();
                    }

                    @Override
                    public boolean nextTerm() {
                        return delegate.nextTerm();
                    }

                    @Override
                    public long termDocFreq() {
                        return delegate.termDocFreq();
                    }

                    @Override
                    public long termIntVal() {
                        return delegate.termIntVal();
                    }

                    @Override
                    public String termStringVal() {
                        return delegate.termStringVal();
                    }

                    @Override
                    public byte[] termStringBytes() {
                        return delegate.termStringBytes();
                    }

                    @Override
                    public int termStringLength() {
                        return delegate.termStringLength();
                    }

                    @Override
                    public boolean nextGroup() {
                        return delegate.nextGroup();
                    }

                    @Override
                    public int group() {
                        return delegate.group();
                    }

                    @Override
                    public void groupStats(final long[] stats) {
                        delegate.groupStats(stats);
                    }

                    @Override
                    public void close() {
                        delegate.close();
                    }
                };
            }
            runThread = new Thread(this, "FTGSSplitterThread-"+threadNameSuffix);
            runThread.setDaemon(true);
        } catch (Throwable t) {
            try {
                close();
            } finally {
                throw Throwables2.propagate(t, IOException.class);
            }
        }
        runThread.start();
    }

    public RawFTGSIterator[] getFtgsIterators() {
        return ftgsIterators;
    }

    public void run() {
        try {
            final RawFTGSIterator rawIterator;
            if (iterator instanceof RawFTGSIterator) {
                rawIterator = (RawFTGSIterator) iterator;
            } else {
                rawIterator = null;
            }
            final long[] statBuf = new long[numStats];
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
                        split = (int)((term*largePrime+12345 & Integer.MAX_VALUE) >> 16)  % numSplits;
                        output = outputs[split];
                        output.switchIntTerm(term, iterator.termDocFreq());
                    } else {
                        if (rawIterator != null) {
                            split = hashStringTerm(rawIterator.termStringBytes(), rawIterator.termStringLength());
                            output = outputs[split];
                            output.switchBytesTerm(rawIterator.termStringBytes(), rawIterator.termStringLength(), rawIterator.termDocFreq());
                        } else {
                            final byte[] termStringBytes = iterator.termStringVal().getBytes(Charsets.UTF_8);
                            split = hashStringTerm(termStringBytes, termStringBytes.length);
                            output = outputs[split];
                            output.switchBytesTerm(termStringBytes, termStringBytes.length, iterator.termDocFreq());
                        }
                    }
                    while (iterator.nextGroup()) {
                        output.switchGroup(iterator.group());
                        iterator.groupStats(statBuf);
                        for (long stat : statBuf) {
                            output.addStat(stat);
                        }
                    }
                }
            }
            for (final FTGSOutputStreamWriter output : outputs) {
                output.close();
            }
        } catch (Throwable t) {
            close();
            throw Throwables.propagate(t);
        } finally {
            Closeables2.closeQuietly(iterator, log);
        }
    }

    private int hashStringTerm(byte[] termStringBytes, int termStringLength) {
        return ((MurmurHash.hash32(termStringBytes, 0, termStringLength)*largePrime+12345 & 0x7FFFFFFF) >> 16) % numSplits;
    }

    @Override
    public void close() {
        if (done.compareAndSet(false, true)) {
            try {
                if (Thread.currentThread() != runThread) {
                    while (true) {
                        try {
                            runThread.interrupt();
                            runThread.join(10);
                            if (!runThread.isAlive()) break;
                        } catch (InterruptedException e) {
                            //ignore
                        }
                    }
                }
            } finally {
                Closeables2.closeAll(log, iterator, Closeables2.forIterable(log, Iterables.transform(Arrays.asList(files), new Function<File, Closeable>() {
                    public Closeable apply(final File input) {
                        return new Closeable() {
                            public void close() throws IOException {
                                input.delete();
                            }
                        };
                    }
                })), Closeables2.forArray(log, outputs), Closeables2.forArray(log, ftgsIterators), Closeables2.forArray(log, outputStreams));
            }
        }
    }

    public boolean isClosed() {
        return done.get();
    }
}

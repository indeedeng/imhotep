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
 package com.indeed.imhotep.service;

import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.indeed.util.core.io.Closeables2;
import com.indeed.imhotep.api.DocIterator;
import com.indeed.imhotep.io.CircularIOStream;

import org.apache.log4j.Logger;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * @author jplaisance
 */
public final class DocIteratorMerger implements DocIterator {
    private static final Logger log = Logger.getLogger(DocIteratorMerger.class);

    private final CircularIOStream circularBuffer;
    private final InputStreamDocIterator iterator;
    private final List<DocIterator> iterators;
    private final ExecutorService executorService = Executors.newCachedThreadPool();

    public DocIteratorMerger(List<DocIterator> iterators, final int numIntFields, final int numStringFields) throws IOException {
        circularBuffer = new CircularIOStream(16384);
        this.iterators = iterators;
        final DataOutputStream dataOut = new DataOutputStream(circularBuffer.getOutputStream());
        final List<Future<Void>> futures = Lists.newArrayList();

        for (final DocIterator docIterator : iterators) {
            futures.add(
                    executorService.submit(new Callable<Void>() {
                        public Void call() throws Exception {
                            DocOutputStreamWriter.writeThreadSafe(docIterator, numIntFields, numStringFields, dataOut);
                            return null;
                        }
                    }
            ));
        }
        final Thread monitorThread = new Thread(new Runnable() {
            public void run() {
                try {
                    for (Future<Void> future : futures) {
                        try {
                            future.get();
                        } catch (ExecutionException e) {
                            log.error("error", e.getCause());
                            synchronized (dataOut) {
                                dataOut.writeByte(0);
                            }
                            close();
                        }
                    }
                    dataOut.writeByte(0);
                    dataOut.close();
                } catch (InterruptedException e) {
                    throw Throwables.propagate(e);
                } catch (IOException e) {
                    throw Throwables.propagate(e);
                }
            }
        });
        monitorThread.start();
        iterator = new InputStreamDocIterator(circularBuffer.getInputStream(), numIntFields, numStringFields);
    }

    public boolean next() {
        return iterator.next();
    }

    public int getGroup() {
        return iterator.getGroup();
    }

    public long getInt(final int index) {
        return iterator.getInt(index);
    }

    public String getString(final int index) {
        return iterator.getString(index);
    }

    public synchronized void close() {
        try {
            Closeables2.closeAll(log, circularBuffer.getInputStream(), circularBuffer.getOutputStream(), iterator);
        } finally {
            try {
                executorService.shutdownNow();
                executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                throw Throwables.propagate(e);
            } finally {
                Closeables2.closeAll(log, iterators);
            }
        }
    }
}

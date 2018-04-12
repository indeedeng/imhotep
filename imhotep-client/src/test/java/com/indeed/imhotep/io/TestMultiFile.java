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

package com.indeed.imhotep.io;

import com.google.common.base.Throwables;
import com.indeed.util.core.shell.PosixFileOperations;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Random;
import java.util.concurrent.CountDownLatch;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author jplaisance
 */
public final class TestMultiFile {
    private static final Logger log = Logger.getLogger(TestMultiFile.class);

    private File tmpDir;

    @Before
    public void setUp() throws IOException {
        tmpDir = File.createTempFile("tmp", "", new File("."));
        tmpDir.delete();
        tmpDir.mkdir();
        System.out.println(tmpDir.getAbsolutePath());
    }

    @After
    public void tearDown() throws IOException {
        PosixFileOperations.rmrf(tmpDir);
    }

    @Test
    public void testStuff() throws IOException, InterruptedException {

        final int splits = 17;
        for (int i = 1; i <= 256*1024; i*=2) {
            System.out.println("running test with read/write size of "+i+" bytes");
            runTest(splits, createData(splits), i, false);
        }
        for (int i = 1; i <= 256*1024; i*=2) {
            System.out.println("running test with read/write size of 1-"+i+" bytes, randomized");
            runTest(splits, createData(splits), i, true);
        }
    }

    private byte[][] createData(final int splits) throws IOException {
        final byte[][] splitData = new byte[splits][];
        for (int i = 0; i < splits; i++) {
            final Random r = new Random();
            final int size = r.nextInt(256*1024)+1;
            final ByteArrayOutputStream out = new ByteArrayOutputStream();
            for (int j = 0; j < size; j++) {
                out.write(r.nextInt() & 0xFF);
            }
            splitData[i] = out.toByteArray();
        }
        return splitData;
    }

    public void runTest(final int splits, final byte[][] splitData, final int maxCopySize, final boolean randomize) throws IOException, InterruptedException {
        final File file = File.createTempFile("tmp", ".bin", tmpDir);
        final MultiFile multiFile = MultiFile.create(file, splits, 4096);
        file.delete();
        final CountDownLatch latch = new CountDownLatch(splits);
        for (int i = 0; i < splits; i++) {
            final OutputStream out = multiFile.getOutputStream(i);
            final int splitIndex = i;
            new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        final byte[] split = splitData[splitIndex];
                        final Random r = new Random();
                        int off = 0;
                        while (off < split.length) {
                            if (maxCopySize == 1) {
                                out.write(split[off]&0xFF);
                                off++;
                            } else {
                                final int copySize = randomize ? r.nextInt(maxCopySize)+1 : maxCopySize;
                                out.write(split, off, Math.min(copySize, split.length-off));
                                off += copySize;
                            }
                        }
                        out.close();
                        latch.countDown();
                    } catch (final Throwable t) {
                        log.error("error", t);
                        throw Throwables.propagate(t);
                    }
                }
            }).start();
        }
        latch.await();
        final CountDownLatch latch2 = new CountDownLatch(splits);
        for (int i = 0; i < splits; i++) {
            final InputStream in = multiFile.getInputStream(i);
            final int splitIndex = i;
            new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        final byte[] split = splitData[splitIndex];
                        final byte[] input = new byte[maxCopySize];
                        final Random r = new Random();
                        int off = 0;
                        while (off < split.length) {
                            if (maxCopySize == 1) {
                                final int b = in.read();
                                assertTrue(b >= 0);
                                assertTrue(b < 256);
                                assertEquals("mismatch at offset " + off, split[off] & 0xFF, b);
                                off++;
                            } else {
                                final int copySize = randomize ? r.nextInt(maxCopySize)+1 : maxCopySize;
                                final int bytesRead = in.read(input, 0, copySize);
                                assertTrue(bytesRead >= 0);
                                for (int i = 0; i < bytesRead; i++) {
                                    assertEquals(split[off], input[i]);
                                    off++;
                                }
                            }
                        }
                        if (maxCopySize == 1) {
                            assertEquals(-1, in.read());
                        } else {
                            assertEquals(-1, in.read(input));
                        }
                        in.close();
                        latch2.countDown();
                    } catch (final Throwable t) {
                        log.error("error", t);
                        throw Throwables.propagate(t);
                    }
                }
            }).start();
        }
        latch2.await();
    }
}

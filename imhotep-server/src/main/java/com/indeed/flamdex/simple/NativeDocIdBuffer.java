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

import com.google.common.io.ByteStreams;
import org.apache.log4j.Logger;
import sun.misc.Unsafe;

import java.io.Closeable;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Field;

/**
 * @author jplaisance
 */
public final class NativeDocIdBuffer implements Closeable {

    private static final Logger log = Logger.getLogger(NativeDocIdBuffer.class);

    private static final Unsafe UNSAFE;
    private static final long INT_ARRAY_BASE_OFFSET;

    private static final int BUFFER_LENGTH = 512;

    private final boolean useSSSE3;

    static {
        loadNativeLibrary();
        nativeInit();
        log.info("libvarint loaded");
    }

    static {
        try {
            final Field theUnsafe = Unsafe.class.getDeclaredField("theUnsafe");
            theUnsafe.setAccessible(true);
            UNSAFE = (Unsafe)theUnsafe.get(null);
            INT_ARRAY_BASE_OFFSET = UNSAFE.arrayBaseOffset(int[].class);
        } catch (final NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    static void loadNativeLibrary() {
        try {
            final String osName = System.getProperty("os.name");
            final String arch = System.getProperty("os.arch");
            final String resourcePath = "/native/" + osName + "-" + arch + "/libvarint.so.1.0.1";
            final InputStream is = NativeDocIdStream.class.getResourceAsStream(resourcePath);
            if (is == null) {
                throw new FileNotFoundException("unable to find libvarint.so.1.0.1 at resource path "+resourcePath);
            }
            final File tempFile = File.createTempFile("libvarint", ".so");
            final OutputStream os = new FileOutputStream(tempFile);
            ByteStreams.copy(is, os);
            os.close();
            is.close();
            System.load(tempFile.getAbsolutePath());
            // noinspection ResultOfMethodCallIgnored
            tempFile.delete();
        } catch (final Throwable e) {
            log.warn("unable to load libvarint using class loader, looking in java.library.path", e);
            System.loadLibrary("varint"); // if this fails it throws UnsatisfiedLinkError
        }
    }

    private final long bufAddress;
    private int bufIndex;
    private int bufLen;

    private long position;

    private long docsRemaining;

    NativeDocIdBuffer(final boolean useSSSE3) {
        bufAddress = UNSAFE.allocateMemory(4 * BUFFER_LENGTH);
        this.useSSSE3 = useSSSE3;
    }

    public void reset(final long newPosition, final long newDocsRemaining) {
        position = newPosition;
        docsRemaining = newDocsRemaining;
        // to force a refill
        bufIndex = 0;
        bufLen = 0;
    }

    public int fillDocIdBuffer(final int[] docIdBuffer, final int limit) {
        int off = 0;
        do {
            if (bufLen == bufIndex){
                if (docsRemaining == 0) {
                    break;
                }
                readInts();
            }
            final int n = Math.min(limit-off, bufLen-bufIndex);
            UNSAFE.copyMemory(null, bufAddress + bufIndex * 4, docIdBuffer, INT_ARRAY_BASE_OFFSET + off * 4, n * 4);
            off += n;
            bufIndex += n;
        } while (off < limit);
        return off;
    }

    private void readInts() {

        final int length = (int)Math.min(docsRemaining, BUFFER_LENGTH);
        if (useSSSE3) {
            position += readInts(position, bufAddress, length);
        } else {
            position += readIntsSingle(position, bufAddress, length);
        }
        docsRemaining -= length;
        bufIndex = 0;
        bufLen = length;
    }

    @Override
    public void close() throws IOException {
        UNSAFE.freeMemory(bufAddress);
    }

    private static native long readInts(long bytesAddr, long intsAddr, int length);

    private static native long readIntsSingle(long bytesAddr, long intsAddr, int length);

    private static native void nativeInit();
}

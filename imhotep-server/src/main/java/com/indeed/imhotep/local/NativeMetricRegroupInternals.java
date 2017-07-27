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
 package com.indeed.imhotep.local;

import com.indeed.util.core.nativelibs.NativeLibraryUtils;
import com.indeed.util.mmap.DirectMemory;
import org.apache.log4j.Logger;
import sun.misc.Unsafe;

import java.lang.reflect.Field;

/**
 * @author jplaisance
 */
public final class NativeMetricRegroupInternals {
    private NativeMetricRegroupInternals() {
    }

    private static final Logger log = Logger.getLogger(NativeMetricRegroupInternals.class);

    private static final Unsafe UNSAFE;
    private static final long INT_ARRAY_BASE_OFFSET;

    static {
        try {
            NativeLibraryUtils.loadLibrary("metricregroup", "1.0.1");
            final Field theUnsafe = Unsafe.class.getDeclaredField("theUnsafe");
            theUnsafe.setAccessible(true);
            UNSAFE = (Unsafe)theUnsafe.get(null);
            INT_ARRAY_BASE_OFFSET = UNSAFE.arrayBaseOffset(int[].class);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    public static void calculateGroups(
            final int min,
            final int max,
            final long magicNumber,
            final int numBuckets,
            final int n,
            final int[] valBuf,
            final int[] docGroupBuffer,
            final DirectMemory nativeValBuf,
            final DirectMemory nativeDocGroupBuffer) {
        final long valBufAddress = nativeValBuf.getAddress();
        UNSAFE.copyMemory(valBuf, INT_ARRAY_BASE_OFFSET, null, valBufAddress, 4*n);
        final long docGroupBufferAddress = nativeDocGroupBuffer.getAddress();
        calculateGroups(min, max, magicNumber, numBuckets, n, valBufAddress, docGroupBufferAddress);
        UNSAFE.copyMemory(null, docGroupBufferAddress, docGroupBuffer, INT_ARRAY_BASE_OFFSET, 4*n);
    }

    public static native long getMagicNumber(int divisor);

    public static native void calculateGroups(final int min, final int max, final long magicNumber, final int numBuckets, final int n, long nativeValBufAddress, long nativeDocGroupBufferAddress);
}

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
 package com.indeed.flamdex.datastruct;

import com.indeed.util.mmap.DirectMemory;
import com.indeed.util.mmap.LongArray;
import com.indeed.util.mmap.MMapBuffer;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;

/**
 * @author jsgroth
 *
 * copy-paste of FastBitSet that operates on an mmap'd file instead of a long array
 */
public final class MMapFastBitSet implements Closeable {
    private final int size;

    private final int arraySize;
    private final MMapBuffer buffer;
    private final int bufferLength;
    private final LongArray bits;

    public MMapFastBitSet(File file, int size, FileChannel.MapMode mapMode) throws IOException {
        this.size = size;
        this.arraySize = (size + 64) >> 6;
        this.bufferLength = arraySize * 8;
        this.buffer = new MMapBuffer(file, 0, bufferLength, mapMode, ByteOrder.LITTLE_ENDIAN);
        this.bits = buffer.memory().longArray(0, arraySize);
    }

    @Deprecated
    public DirectMemory getBackingMemory() {
        return this.buffer.memory();
    }

    public final boolean get(final int i) {
        return (bits.get(i >> 6) & (1L << (i & 0x3F))) != 0;
    }

    public final void set(final int i) {
        bits.set(i >> 6, bits.get(i >> 6) | (1L << (i & 0x3F)));
    }

    public final void clear(final int i) {
        bits.set(i >> 6, bits.get(i >> 6) & ~(1L << (i & 0x3F)));
    }

    public final void set(final int i, final boolean v) {
        if (v)
            set(i);
        else
            clear(i);
    }

    public final void setRange(final int b, final int e) {
        final int bt = b >> 6;
        final int et = e >> 6;
        if (bt != et) {
            fill(bits, bt + 1, et, -1L);
            bits.set(bt, bits.get(bt) | (-1L << (b & 0x3F)));
            bits.set(et, bits.get(et) | ~(-1L << (e & 0x3F)));
        } else {
            bits.set(bt, bits.get(bt) | ((-1L << (b & 0x3F)) & ~(-1L << (e & 0x3F))));
        }
    }

    public final void clearRange(final int b, final int e) {
        final int bt = b >> 6;
        final int et = e >> 6;
        if (bt != et) {
            fill(bits, bt + 1, et, 0L);
            bits.set(bt, bits.get(bt) & ~(-1L << (b & 0x3F)));
            bits.set(et, bits.get(et) & (-1L << (e & 0x3F)));
        } else {
            bits.set(bt, bits.get(bt) & (~(-1L << (b & 0x3F)) | (-1L << (e & 0x3F))));
        }
    }

    public final void setAll() {
        fill(bits, 0, arraySize, -1L);
    }

    public final void clearAll() {
        fill(bits, 0, arraySize, 0L);
    }

    public final void invertAll() {
        for (int i = 0; i < arraySize; ++i)
            bits.set(i, ~bits.get(i));
    }

    public final void and(final MMapFastBitSet other) {
        for (int i = 0; i < arraySize; ++i)
            bits.set(i, bits.get(i) & other.bits.get(i));
    }

    public final void or(final MMapFastBitSet other) {
        for (int i = 0; i < arraySize; ++i)
            bits.set(i, bits.get(i) | other.bits.get(i));
    }

    public final void nand(final MMapFastBitSet other) {
        for (int i = 0; i < arraySize; ++i)
            bits.set(i, ~(bits.get(i) & other.bits.get(i)));
    }

    public final void nor(final MMapFastBitSet other) {
        for (int i = 0; i < arraySize; ++i)
            bits.set(i, ~(bits.get(i) | other.bits.get(i)));
    }

    public final void xor(final MMapFastBitSet other) {
        for (int i = 0; i < arraySize; ++i)
            bits.set(i, bits.get(i) ^ other.bits.get(i));
    }

    public final int cardinality() {
        if (size == 0) return 0;
        int count = 0;
        for (int i = 0; i < arraySize - 1; ++i)
            count += Long.bitCount(bits.get(i));
        return count + Long.bitCount(bits.get(arraySize - 1) & ~(-1L << (size & 0x3F)));
    }

    public final int size() {
        return size;
    }

    public final long memoryUsage() {
        return 0;
    }

    private static void fill(final LongArray a, final int b, final int e, final long l) {
        for (int i = b; i < e; ++i)
            a.set(i, l);
    }

    public void sync() throws IOException {
        buffer.sync(0, bufferLength);
    }

    @Override
    public void close() throws IOException {
        buffer.close();
    }
}

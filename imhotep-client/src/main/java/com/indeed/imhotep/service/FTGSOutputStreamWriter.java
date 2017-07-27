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

import com.google.common.base.Charsets;
import com.indeed.imhotep.api.FTGSIterator;
import com.indeed.imhotep.api.RawFTGSIterator;
import com.indeed.util.io.VIntUtils;

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;

public final class FTGSOutputStreamWriter implements Closeable {
    private final OutputStream out;

    private boolean fieldIsIntType;

    private byte[] previousTermBytes = new byte[100];
    private int previousTermLength;
    private byte[] currentTermBytes = new byte[100];
    private int currentTermLength;
    private long previousTermInt;
    private long currentTermInt;

    private long currentTermDocFreq;

    private boolean termWritten;
    private boolean fieldWritten = false;

    private int previousGroupId = -1;

    public FTGSOutputStreamWriter(final OutputStream out) {
        this.out = out;
    }

    public void switchField(final String field, final boolean isIntType) throws IOException {
        endField();
        fieldIsIntType = isIntType;
        startField(fieldIsIntType, field, out);
        fieldWritten = true;
        previousTermLength = 0;
        previousTermInt = -1;
    }

    public void switchBytesTerm(final byte[] termBytes, final int termLength, final long termDocFreq) throws IOException {
        endTerm();
        currentTermBytes = copyInto(termBytes, termLength, currentTermBytes);
        currentTermLength = termLength;
        currentTermDocFreq = termDocFreq;
    }

    public void switchIntTerm(final long term, final long termDocFreq) throws IOException {
        endTerm();
        currentTermInt = term;
        currentTermDocFreq = termDocFreq;
    }

    public void switchGroup(final int groupId) throws IOException {
        if (!termWritten) {
            writeTerm();
        }
        writeVLong(groupId - previousGroupId, out);
        previousGroupId = groupId;
    }

    private void writeTerm() throws IOException {
        if (fieldIsIntType) {
            if (previousTermInt == -1 && currentTermInt == previousTermInt) {
                //still decodes to 0 but allows reader to distinguish between end of field and delta of zero
                out.write(0x80);
                out.write(0);
            } else {
                writeVLong(currentTermInt - previousTermInt, out);
            }
            previousTermInt = currentTermInt;
        } else {
            final int pLen = prefixLen(previousTermBytes, currentTermBytes, Math.min(previousTermLength, currentTermLength));
            writeVLong((previousTermLength - pLen) + 1, out);
            writeVLong(currentTermLength - pLen, out);
            out.write(currentTermBytes, pLen, currentTermLength - pLen);
            previousTermBytes = copyInto(currentTermBytes, currentTermLength, previousTermBytes);
            previousTermLength = currentTermLength;
        }
        writeSVLong(currentTermDocFreq, out);
        termWritten = true;
    }

    public void addStat(final long stat) throws IOException {
        writeSVLong(stat, out);
    }

    public void close() throws IOException {
        endField();
        out.write(0);
        out.flush();
    }

    private void endField() throws IOException {
        if (!fieldWritten) {
            return;
        }
        endTerm();
        out.write(0);
        if (!fieldIsIntType) {
            out.write(0);
        }
    }

    private void endTerm() throws IOException {
        if (termWritten) {
            out.write(0);
        }
        termWritten = false;
        previousGroupId = -1;
    }

    public static void write(final FTGSIterator buffer, final int numStats, final OutputStream out) throws IOException {
        final FTGSOutputStreamWriter writer = new FTGSOutputStreamWriter(out);
        writer.write(buffer, numStats);
    }

    public void write(final FTGSIterator buffer, final int numStats) throws IOException {
        final long[] stats = new long[numStats];
        if (buffer instanceof RawFTGSIterator) {
            final RawFTGSIterator rawBuffer = (RawFTGSIterator)buffer;
            while (rawBuffer.nextField()) {
                final boolean fieldIsIntType = rawBuffer.fieldIsIntType();
                switchField(rawBuffer.fieldName(), fieldIsIntType);
                while (rawBuffer.nextTerm()) {
                    if (fieldIsIntType) {
                        switchIntTerm(rawBuffer.termIntVal(), rawBuffer.termDocFreq());
                    } else {
                        // termStringBytes() returns a reference so this copies the bytes instead of hanging on to it
                        switchBytesTerm(rawBuffer.termStringBytes(), rawBuffer.termStringLength(), rawBuffer.termDocFreq());
                    }
                    while (rawBuffer.nextGroup()){
                        switchGroup(rawBuffer.group());
                        rawBuffer.groupStats(stats);
                        for (final long stat : stats) {
                            addStat(stat);
                        }
                    }
                    endTerm();
                }
            }
        } else {
            while (buffer.nextField()) {
                final boolean fieldIsIntType = buffer.fieldIsIntType();
                switchField(buffer.fieldName(), fieldIsIntType);
                while (buffer.nextTerm()) {
                    if (fieldIsIntType) {
                        switchIntTerm(buffer.termIntVal(), buffer.termDocFreq());
                    } else {
                        final byte[] bytes = buffer.termStringVal().getBytes(Charsets.UTF_8);
                        switchBytesTerm(bytes, bytes.length, buffer.termDocFreq());
                    }
                    while (buffer.nextGroup()){
                        switchGroup(buffer.group());
                        buffer.groupStats(stats);
                        for (final long stat : stats) {
                            addStat(stat);
                        }
                    }
                    endTerm();
                }
            }
        }
        close();
    }

    private static void writeVLong(final long i, final OutputStream out) throws IOException {
        VIntUtils.writeVInt64(out, i);
    }

    private static void writeSVLong(final long i, final OutputStream out) throws IOException {
        VIntUtils.writeSVInt64(out, i);
    }

    private static void startField(final boolean isIntType, final String field, final OutputStream out) throws IOException {
        if (isIntType) {
            out.write(1);
        } else {
            out.write(2);
        }
        final byte[] fieldBytes = field.getBytes(Charsets.UTF_8);
        writeVLong(fieldBytes.length, out);
        out.write(fieldBytes);
    }

    private static byte[] copyInto(final byte[] src, final int srcLen, byte[] dest) {
        dest = ensureCap(dest, srcLen);
        System.arraycopy(src, 0, dest, 0, srcLen);
        return dest;
    }

    private static byte[] ensureCap(final byte[] b, final int len) {
        if (b == null) {
            return new byte[Math.max(len, 16)];
        }
        if (b.length >= len) {
            return b;
        }
        return new byte[Math.max(b.length*2, len)];
    }

    private static int prefixLen(final byte[] a, final byte[] b, final int max) {
        for (int i = 0; i < max; i++) {
            if (a[i] != b[i]) {
                return i;
            }
        }
        return max;
    }
}

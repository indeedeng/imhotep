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
 package com.indeed.imhotep.service;

import com.indeed.imhotep.FTGSBinaryFormat;
import com.indeed.imhotep.FTGSBinaryFormat.FieldStat;
import com.indeed.imhotep.StreamUtil.OutputStreamWithPosition;
import com.indeed.imhotep.api.AggregateFTGSIterator;
import com.indeed.imhotep.api.FTGIterator;
import com.indeed.imhotep.api.FTGSIterator;

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public final class FTGSOutputStreamWriter implements Closeable {
    private final OutputStreamWithPosition out;

    private boolean fieldIsIntType;

    private final ByteBuffer doubleByteBuffer = ByteBuffer.wrap(new byte[8]);

    private byte[] previousTermBytes = new byte[100];
    private int previousTermLength;
    private byte[] currentTermBytes = new byte[100];
    private int currentTermLength;
    private long previousTermInt;
    private long currentTermInt;
    private String currentField;
    private FieldStat currentStat;
    private final List<FieldStat> fieldStats = new ArrayList<>();
    private boolean closed = false;

    private long currentTermDocFreq;

    private boolean termWritten;
    private boolean fieldWritten = false;

    private int previousGroupId = -1;

    public FTGSOutputStreamWriter(final OutputStream out) {
        this.out = new OutputStreamWithPosition(out);
    }

    public void switchField(final String field, final boolean isIntType) throws IOException {
        endField();
        fieldIsIntType = isIntType;
        FTGSBinaryFormat.writeFieldStart(fieldIsIntType, field, out);
        fieldWritten = true;
        previousTermLength = 0;
        previousTermInt = -1;
        currentField = field;
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
        FTGSBinaryFormat.writeGroup(groupId, previousGroupId, out);
        previousGroupId = groupId;
    }

    private void writeTerm() throws IOException {
        if (currentStat == null) {
            // first term is current field.
            currentStat = new FieldStat(currentField, fieldIsIntType);
            currentStat.startPosition = out.getPosition();
            if (fieldIsIntType) {
                currentStat.firstIntTerm = currentTermInt;
            } else {
                currentStat.firstStringTerm = makeCopy(currentTermBytes, currentTermLength);
            }
        }

        if (fieldIsIntType) {
            FTGSBinaryFormat.writeIntTermStart(currentTermInt, previousTermInt, out);
            previousTermInt = currentTermInt;
        } else {
            FTGSBinaryFormat.writeStringTermStart(currentTermBytes, currentTermLength, previousTermBytes, previousTermLength, out);
            previousTermBytes = copyInto(currentTermBytes, currentTermLength, previousTermBytes);
            previousTermLength = currentTermLength;
        }
        FTGSBinaryFormat.writeTermDocFreq(currentTermDocFreq, out);
        termWritten = true;
    }

    public void addStat(final long stat) throws IOException {
        FTGSBinaryFormat.writeStat(stat, out);
    }

    public void addStat(final double stat) throws IOException {
        doubleByteBuffer.putDouble(0, stat);
        out.write(doubleByteBuffer.array());
    }

    public FieldStat[] closeAndGetStats() throws IOException {
        if (!closed) {
            closed = true;
            endField();
            FTGSBinaryFormat.writeFtgsEndTag(out);
            out.flush();
        }
        return fieldStats.toArray(new FieldStat[fieldStats.size()]);
    }

    private void endField() throws IOException {
        if (!fieldWritten) {
            return;
        }
        endTerm();
        if (currentStat != null) {
            if (fieldIsIntType) {
                currentStat.lastIntTerm = previousTermInt;
            } else {
                currentStat.lastStringTerm = makeCopy(previousTermBytes, previousTermLength);
            }
        } else {
            currentStat = new FieldStat(currentField, fieldIsIntType);
            currentStat.startPosition = out.getPosition();
        }

        currentStat.endPosition = out.getPosition();
        fieldStats.add(currentStat);
        currentStat = null;

        fieldWritten = false;
        FTGSBinaryFormat.writeFieldEnd(fieldIsIntType, out);
    }

    private void endTerm() throws IOException {
        if (termWritten) {
            FTGSBinaryFormat.writeGroupStatsEnd(out);
        }
        termWritten = false;
        previousGroupId = -1;
    }

    public static FieldStat[] write(final FTGSIterator buffer, final OutputStream out) throws IOException {
        final FTGSOutputStreamWriter writer = new FTGSOutputStreamWriter(out);
        return writer.write(buffer);
    }

    public static FieldStat[] write(final AggregateFTGSIterator buffer, final OutputStream out) throws IOException {
        final FTGSOutputStreamWriter writer = new FTGSOutputStreamWriter(out);
        return writer.write(buffer);
    }

    private interface StatsWriter {
        void writeStats() throws IOException;
    }

    public FieldStat[] write(final FTGIterator iterator, final StatsWriter statsWriter) throws IOException {
        while (iterator.nextField()) {
            final boolean fieldIsIntType = iterator.fieldIsIntType();
            switchField(iterator.fieldName(), fieldIsIntType);
            while (iterator.nextTerm()) {
                if (fieldIsIntType) {
                    switchIntTerm(iterator.termIntVal(), iterator.termDocFreq());
                } else {
                    // termStringBytes() returns a reference so this copies the bytes instead of hanging on to it
                    switchBytesTerm(iterator.termStringBytes(), iterator.termStringLength(), iterator.termDocFreq());
                }
                while (iterator.nextGroup()){
                    switchGroup(iterator.group());
                    statsWriter.writeStats();
                }
                endTerm();
            }
        }
        return closeAndGetStats();
    }

    public FieldStat[] write(final FTGSIterator buffer) throws IOException {
        final long[] stats = new long[buffer.getNumStats()];
        final StatsWriter statsWriter = () -> {
            buffer.groupStats(stats);
            for (final long stat : stats) {
                this.addStat(stat);
            }
        };
        return write(buffer, statsWriter);
    }

    public FieldStat[] write(final AggregateFTGSIterator buffer) throws IOException {
        final double[] stats = new double[buffer.getNumStats()];
        final StatsWriter statsWriter = () -> {
            buffer.groupStats(stats);
            for (final double stat : stats) {
                this.addStat(stat);
            }
        };
        return write(buffer, statsWriter);
    }

    private static byte[] copyInto(final byte[] src, final int srcLen, byte[] dest) {
        dest = ensureCap(dest, srcLen);
        System.arraycopy(src, 0, dest, 0, srcLen);
        return dest;
    }

    private static byte[] makeCopy(final byte[] src, final int srcLen) {
        final byte[] dest = new byte[srcLen];
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

    @Override
    public void close() throws IOException {
        closeAndGetStats();
    }

}

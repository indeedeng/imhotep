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

import com.google.common.base.Charsets;
import com.indeed.imhotep.FTGSBinaryFormat.FieldStat;
import com.indeed.imhotep.api.FTGIterator;
import com.indeed.util.core.Pair;
import com.indeed.util.core.io.Closeables2;
import org.apache.log4j.Logger;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CharsetDecoder;
import java.util.Optional;

public abstract class AbstractInputStreamFTGSIterator implements FTGIterator {

    private static final Logger log = Logger.getLogger(AbstractInputStreamFTGSIterator.class);

    private final byte[] buffer = new byte[8192];
    private int bufferPtr = 0;
    private int bufferLen = 0;

    private byte readByte() throws IOException {
        if (bufferPtr == bufferLen) {
            refillBuffer();
        }
        return buffer[bufferPtr++];
    }

    void readBytes(final byte[] b, int off, int len) throws IOException {
        while (len > 0) {
            if (bufferPtr == bufferLen) {
                refillBuffer();
            }
            final int toCopy = Math.min(len, bufferLen - bufferPtr);
            try {
                System.arraycopy(buffer, bufferPtr, b, off, toCopy);
            } catch(final ArrayIndexOutOfBoundsException e) {
                e.printStackTrace();
            }
            bufferPtr += toCopy;
            off += toCopy;
            len -= toCopy;
        }
    }

    private void refillBuffer() throws IOException {
        bufferLen = in.read(buffer);
        if (bufferLen == -1) {
            throw new IOException("Unexpected end of stream");
        }
        bufferPtr = 0;
    }

    private int readVInt() throws IOException {
        int ret = 0;
        int shift = 0;
        while (true) {
            final byte val = readByte();
            ret += (val&0x7F)<<shift;
            if (val >= 0) {
                break;
            }
            shift += 7;
        }
        return ret;
    }

    private long readVLong(final byte firstByte) throws IOException {
        long ret = 0;
        int shift = 0;
        byte val = firstByte;
        while (true) {
            ret += (val&0x7FL)<<shift;
            if (val >= 0) {
                break;
            }
            shift += 7;
            val = readByte();
        }
        return ret;
    }

    long readSVLong() throws IOException {
        final long ret = readVLong(readByte());
        return (ret >>> 1) ^ -(ret & 1);
    }

    int iteratorStatus = 1; // 0 = end, 1 = reading fields, 2 = reading terms, 3 = reading groups
    private final InputStream in;
    private final int numGroups;
    private final Optional<FieldStat[]> fieldsStats;

    public AbstractInputStreamFTGSIterator(final InputStream in,
                                           @Nullable final FieldStat[] fieldsStats,
                                           final int numGroups) {
        this.in = in;
        this.fieldsStats = Optional.ofNullable(fieldsStats);
        this.numGroups = numGroups;
    }

    public Pair<InputStream, Optional<FieldStat[]>> getStreamAndStats() {
        return Pair.of(in, fieldsStats);
    }

    private String fieldName;
    private boolean fieldIsIntType;

    private long termDocFreq;
    private long intTermVal;
    private String stringTermVal;

    // needed to receive byte[] deltas over inputstream
    private byte[] currentTermBytes = new byte[16];
    private ByteBuffer byteBuffer = ByteBuffer.wrap(currentTermBytes);
    private int currentTermLength;
    private int groupId = -1;

    private final CharsetDecoder decoder = Charsets.UTF_8.newDecoder();

    @Override
    public int getNumGroups() {
        return numGroups;
    }

    @Override
    public boolean nextField() {
        if (iteratorStatus < 1) {
            return false;
        }

        while (nextTerm()) {
            // skip until end of current field reached....
        }
        // try to read next field from input stream...
        try {
            internalNextField();
        } catch (final IOException e) {
            iteratorStatus = -1;
            throw new RuntimeException(e);
        }

        return iteratorStatus == 2;
    }

    private void internalNextField() throws IOException {
        final int fieldType = readByte() & 0xFF;
        if (fieldType == 0) {
            iteratorStatus = 0;
            return; // normal end of stream condition
        }

        fieldIsIntType = fieldType == 1;
        final int fieldNameLength = readVInt();
        final byte[] fieldNameBytes = new byte[fieldNameLength];
        readBytes(fieldNameBytes, 0, fieldNameBytes.length);
        fieldName = new String(fieldNameBytes, Charsets.UTF_8);
        intTermVal = -1;
        stringTermVal = null;
        currentTermLength = 0;
        groupId = -1;
        iteratorStatus = 2;
    }

    @Override
    public final String fieldName() {
        return fieldName;
    }

    @Override
    public final boolean fieldIsIntType() {
        return fieldIsIntType;
    }

    @Override
    public boolean nextTerm() {
        if (iteratorStatus < 2) {
            return false;
        }
        while (nextGroup()) {
            // skip until end of current term
        }

        try {
            internalNextTerm();
        } catch (final IOException e) {
            iteratorStatus = -1;
            throw new RuntimeException(e);
        }
        return iteratorStatus == 3;
    }

    private void internalNextTerm() throws IOException {
        if (fieldIsIntType) {
            final byte firstByte = readByte();
            if (firstByte == 0) {
                iteratorStatus = 1;
                return;
            }
            intTermVal += readVLong(firstByte);
        } else {
            final int removeLengthPlusOne = readVInt();
            final int addLength = readVInt();
            if (removeLengthPlusOne == 0 && addLength == 0) {
                iteratorStatus = 1;
                return;
            }
            final int removeLength = removeLengthPlusOne - 1;
            final int newLength = currentTermLength - removeLength + addLength;

            if (currentTermBytes.length < newLength) {
                final byte[] temp = new byte[Math.max(currentTermBytes.length*2, newLength)];
                System.arraycopy(currentTermBytes, 0, temp, 0, currentTermLength);
                currentTermBytes = temp;
                byteBuffer = ByteBuffer.wrap(currentTermBytes);
            }

            readBytes(currentTermBytes, currentTermLength - removeLength, addLength);
            currentTermLength = newLength;
            stringTermVal = null;
        }
        termDocFreq = readSVLong();
        groupId = -1;
        iteratorStatus = 3;
    }

    @Override
    public final long termDocFreq() {
        return termDocFreq;
    }

    @Override
    public final long termIntVal() {
        return intTermVal;
    }

    @Override
    public final String termStringVal() {
        if (stringTermVal == null) {
            try {
                stringTermVal = decoder.decode((ByteBuffer)byteBuffer.position(0).limit(currentTermLength)).toString();
            } catch (final CharacterCodingException e) {
                throw new RuntimeException(e);
            }
        }
        return stringTermVal;
    }

    @Override
    public final byte[] termStringBytes() {
        return currentTermBytes;
    }

    @Override
    public final int termStringLength() {
        return currentTermLength;
    }

    @Override
    public final boolean nextGroup() {
        if (iteratorStatus < 3) {
            return false;
        }
        try {
            final int grpDelta = readVInt();
            if (grpDelta == 0) {
                iteratorStatus = 2;
            } else {
                groupId += grpDelta;
                readStats();
            }
        } catch (final IOException e) {
            iteratorStatus = -1;
            throw new RuntimeException(e);
        }
        return iteratorStatus == 3;
    }

    abstract void readStats();

    @Override
    public final int group() {
        return groupId;
    }

    @Override
    public void close() {
        Closeables2.closeQuietly(in, log);
    }
}

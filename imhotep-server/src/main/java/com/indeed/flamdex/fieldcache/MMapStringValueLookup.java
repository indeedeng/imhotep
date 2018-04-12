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
 package com.indeed.flamdex.fieldcache;

import com.google.common.base.Charsets;
import com.indeed.util.core.io.Closeables2;
import com.indeed.flamdex.api.StringValueLookup;
import com.indeed.util.mmap.BufferResource;
import com.indeed.util.mmap.Memory;
import org.apache.log4j.Logger;

import javax.annotation.WillCloseWhenClosed;

/**
 * @author jplaisance
 */
public final class MMapStringValueLookup implements StringValueLookup {
    private static final Logger log = Logger.getLogger(MMapStringValueLookup.class);

    private final BufferResource docIdToAddressBuffer;
    private final BufferResource stringValuesBuffer;
    private final Memory docIdToAddress;
    private final Memory stringValues;

    public MMapStringValueLookup(@WillCloseWhenClosed final BufferResource docIdToAddressBuffer,
                                 @WillCloseWhenClosed final BufferResource stringValuesBuffer) {
        this.docIdToAddressBuffer = docIdToAddressBuffer;
        this.stringValuesBuffer = stringValuesBuffer;
        docIdToAddress = docIdToAddressBuffer.memory();
        stringValues = stringValuesBuffer.memory();
    }

    public String getString(final int docId) {
        final int address = docIdToAddress.getInt(docId << 2);
        final int firstByte = stringValues.getByte(address)&0xFF;
        final int length;
        final int valOffset;
        if (firstByte == 0xFF) {
            length = stringValues.getInt(address+1);
            valOffset = address+5;
        } else {
            length = firstByte;
            valOffset = address+1;
        }
        final byte[] bytes = new byte[length];
        stringValues.getBytes(valOffset, bytes);
        return new String(bytes, Charsets.UTF_8);
    }

    public long memoryUsed() {
        return docIdToAddress.length()+stringValues.length();
    }

    public void close() {
        Closeables2.closeQuietly(docIdToAddressBuffer, log);
        Closeables2.closeQuietly(stringValuesBuffer, log);
    }
}

package com.indeed.flamdex.fieldcache;

import com.google.common.base.Charsets;
import com.indeed.util.core.io.Closeables2;
import com.indeed.flamdex.api.StringValueLookup;
import com.indeed.util.mmap.DirectMemory;
import com.indeed.util.mmap.MMapBuffer;
import org.apache.log4j.Logger;

/**
 * @author jplaisance
 */
public final class MMapStringValueLookup implements StringValueLookup {
    private static final Logger log = Logger.getLogger(MMapStringValueLookup.class);

    private final MMapBuffer docIdToAddressBuffer;
    private final MMapBuffer stringValuesBuffer;
    private final DirectMemory docIdToAddress;
    private final DirectMemory stringValues;

    public MMapStringValueLookup(final MMapBuffer docIdToAddressBuffer, final MMapBuffer stringValuesBuffer) {
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

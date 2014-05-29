package com.indeed.imhotep.service;

import com.google.common.base.Charsets;
import org.apache.log4j.Logger;

import java.io.DataOutput;
import java.io.IOException;

/**
 * @author jplaisance
 */
public final class FTGSPipeWriter {
    private static final Logger log = Logger.getLogger(FTGSPipeWriter.class);

    private boolean fieldWritten = false;
    private boolean fieldIsIntType;

    public void switchField(String field, boolean isIntType, DataOutput out) throws IOException {
        endField(out);
        fieldIsIntType = isIntType;
        startField(fieldIsIntType, field, out);
        fieldWritten = true;
    }

    private void startField(boolean isIntType, String field, DataOutput out) throws IOException {
        if (isIntType) {
            out.write(1);
        } else {
            out.write(2);
        }
        final byte[] fieldBytes = field.getBytes(Charsets.UTF_8);
        writeVInt(out, fieldBytes.length);
        out.write(fieldBytes);
    }

    private void writeVInt(DataOutput out, int i) throws IOException {
        do {
            int next = i&0x7F;
            final boolean more = (i & ~0x7F) != 0;
            if (more) next |= 0x80;
            out.write((byte) next);
            if (!more) break;
            i >>>= 7;
        } while (true);
    }

    private void endField(DataOutput out) throws IOException {
        if (!fieldWritten) return;
        out.write(0);
        if (!fieldIsIntType) out.write(0);
    }

    public void close(DataOutput out) throws IOException {
        endField(out);
        out.write(0);
    }
}

package com.indeed.imhotep.service;

import com.google.common.base.Charsets;
import com.google.common.base.Throwables;
import com.indeed.imhotep.api.DocIterator;
import org.apache.log4j.Logger;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * @author jplaisance
 */
public final class InputStreamDocIterator implements DocIterator {
    private static final Logger log = Logger.getLogger(InputStreamDocIterator.class);

    final DataInputStream in;

    private boolean done = false;
    private final long[] intValues;
    private final String[] stringValues;
    private int group;

    public InputStreamDocIterator(InputStream in, int numIntFields, int numStringFields) {
        this.in = new DataInputStream(in);
        intValues = new long[numIntFields];
        stringValues = new String[numStringFields];
    }

    public boolean next() {
        try {
            if (done) return false;
            if (in.readByte() != 1) {
                done = true;
                return false;
            }
            group = in.readInt();
            for (int i = 0; i < intValues.length; i++) {
                intValues[i] = in.readLong();
            }
            for (int i = 0; i < stringValues.length; i++) {
                final int firstByte = in.readByte()&0xFF;
                final int length = firstByte == 0xFF ? in.readInt() : firstByte;
                final byte[] bytes = new byte[length];
                in.readFully(bytes);
                stringValues[i] = new String(bytes, Charsets.UTF_8);
            }
            return true;
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    public int getGroup() {
        return group;
    }

    public long getInt(final int index) {
        return intValues[index];
    }

    public String getString(final int index) {
        return stringValues[index];
    }

    public void close() throws IOException {
        in.close();
    }
}

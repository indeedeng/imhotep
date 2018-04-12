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

    public InputStreamDocIterator(final InputStream in, final int numIntFields, final int numStringFields) {
        this.in = new DataInputStream(in);
        intValues = new long[numIntFields];
        stringValues = new String[numStringFields];
    }

    public boolean next() {
        try {
            if (done) {
                return false;
            }
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
                final int length = (firstByte == 0xFF) ? in.readInt() : firstByte;
                final byte[] bytes = new byte[length];
                in.readFully(bytes);
                stringValues[i] = new String(bytes, Charsets.UTF_8);
            }
            return true;
        } catch (final IOException e) {
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

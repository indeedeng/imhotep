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
 package com.indeed.imhotep.io;

import com.google.common.base.Charsets;
import com.google.common.io.ByteStreams;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * @author jsgroth
 */
public final class Streams {
    private Streams() {}

    public static BufferedInputStream newBufferedInputStream(final InputStream is) {
        return new BufferedInputStream(is, 65536);
    }

    public static BufferedOutputStream newBufferedOutputStream(final OutputStream os) {
        return new BufferedOutputStream(os, 65536);
    }

    public static int readInt(final InputStream is) throws IOException {
        final byte[] buf = new byte[4];
        ByteStreams.readFully(is, buf);
        return Bytes.bytesToInt(buf);
    }

    public static void writeInt(final OutputStream os, final int x) throws IOException {
        final byte[] bytes = Bytes.intToBytes(x);
        os.write(bytes);
    }

    public static long readLong(final InputStream is) throws IOException {
        final byte[] buf = new byte[8];
        ByteStreams.readFully(is, buf);
        return Bytes.bytesToLong(buf);
    }

    public static void writeLong(final OutputStream os, final long x) throws IOException {
        final byte[] bytes = Bytes.longToBytes(x);
        os.write(bytes);
    }

    public static String readUTF8String(final InputStream is) throws IOException {
        final int len = readInt(is);
        return readUTF8String(is, len);
    }

    public static String readUTF8String(final InputStream is, final int len) throws IOException {
        final byte[] bytes = new byte[len];
        ByteStreams.readFully(is, bytes);
        return new String(bytes, Charsets.UTF_8);
    }

    public static void writeUTF8String(final OutputStream os, final String s) throws IOException {
        final byte[] bytes = s.getBytes(Charsets.UTF_8);
        Streams.writeInt(os, bytes.length);
        os.write(bytes);
    }
}

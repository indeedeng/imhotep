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
 package com.indeed.imhotep.io;

import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Random;

import static org.junit.Assert.assertTrue;

/**
 * @author jsgroth
 */
public class TestFileSerializers {
    @Test
    public void testIntArraySerializer() throws IOException {
        final int[] a = newRandomIntArray();
        final int[] a2 = runASerializer(new IntArraySerializer(), a);
        assertTrue(Arrays.equals(a, a2));
    }

//    @Test
//    public void testByteArraySerializer() throws IOException {
//        byte[] a = newRandomByteArray();
//        byte[] a2 = runASerializer(new ByteArraySerializer(), a);
//        assertTrue(Arrays.equals(a, a2));
//    }
//
//    @Test
//    public void testShortArraySerializer() throws IOException {
//        short[] a = newRandomShortArray();
//        short[] a2 = runASerializer(new ShortArraySerializer(), a);
//        assertTrue(Arrays.equals(a, a2));
//    }
//
//    @Test
//    public void testCharArraySerializer() throws IOException {
//        char[] a = newRandomCharArray();
//        char[] a2 = runASerializer(new CharArraySerializer(), a);
//        assertTrue(Arrays.equals(a, a2));
//    }
//
//    @Test
//    public void testLongArraySerializer() throws IOException {
//        long[] a = newRandomLongArray();
//        long[] a2 = runASerializer(new LongArraySerializer(), a);
//        assertTrue(Arrays.equals(a, a2));
//    }

    private <T> T runASerializer(final FileSerializer<T> serializer, final T t) throws IOException {
        final File tmp = File.createTempFile("temp", ".bin");
        try {
            serializer.serialize(t, tmp.toPath());
            return serializer.deserialize(tmp.toPath());
        } finally {
            tmp.delete();
        }
    }

    private static int[] newRandomIntArray() {
        final int[] ret = new int[12500000];
        final Random rand = new Random();
        for (int i = 0; i < ret.length; ++i) {
            ret[i] = rand.nextInt();
        }
        return ret;
    }

    private static byte[] newRandomByteArray() {
        final byte[] ret = new byte[50000000];
        final Random rand = new Random();
        rand.nextBytes(ret);
        return ret;
    }

    private static short[] newRandomShortArray() {
        final short[] ret = new short[25000000];
        final Random rand = new Random();
        for (int i = 0; i < ret.length; ++i) {
            ret[i] = (short)rand.nextInt();
        }
        return ret;
    }

    private static char[] newRandomCharArray() {
        final char[] ret = new char[25000000];
        final Random rand = new Random();
        for (int i = 0; i < ret.length; ++i) {
            ret[i] = (char)rand.nextInt();
        }
        return ret;
    }

    private static long[] newRandomLongArray() {
        final long[] ret = new long[6750000];
        final Random rand = new Random();
        for (int i = 0; i < ret.length; ++i) {
            ret[i] = rand.nextLong();
        }
        return ret;
    }
}
